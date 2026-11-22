"""The pipeline's steps. These are the functions worked out in
notebooks/02_events_bronze_silver_gold, taking the Spark session and the target
schema as arguments so that a job can run them one at a time. Each returns a
small dict of counts for the orchestrator to record against the run."""
import json
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE = "https://raw.githubusercontent.com/hudl/open-data/master/data"
RAW = "/Volumes/workspace/rh_lakehouse/raw"

EVENT_SCHEMA = """
    id string,
    index int,
    period int,
    minute int,
    second int,
    type struct<name: string>,
    team struct<id: int, name: string>,
    player struct<id: int, name: string>,
    location array<double>,
    shot struct<statsbomb_xg: double, outcome: struct<name: string>>
"""

MATCH_SCHEMA = """
    match_id long,
    match_date date,
    match_week int,
    home_team struct<home_team_id: int, home_team_name: string>,
    away_team struct<away_team_id: int, away_team_name: string>,
    home_score int,
    away_score int
"""

IS_SHOT = F.col("event_type") == "Shot"
IS_GOAL = (F.col("shot_outcome") == "Goal") | (F.col("event_type") == "Own Goal For")


def season_filter(competition_id: int, season_id: int) -> str:
    return f"competition_id = {competition_id} AND season_id = {season_id}"


# ---------------------------------------------------------------- land


def land(url: str, path: str) -> int:
    """Download one file into the volume. Returns bytes written, 0 if already there."""
    if os.path.exists(path):
        return 0
    with urllib.request.urlopen(url, timeout=120) as response:
        body = response.read()
    with open(path, "wb") as f:
        f.write(body)
    return len(body)


def land_season(competition_id: int, season_id: int) -> dict:
    """Land the match list and every event file for one competition season."""
    season = f"competition_id={competition_id}/season_id={season_id}"
    os.makedirs(f"{RAW}/matches/{season}", exist_ok=True)
    os.makedirs(f"{RAW}/events/{season}", exist_ok=True)

    matches_path = f"{RAW}/matches/{season}/matches.json"
    land(f"{SOURCE}/matches/{competition_id}/{season_id}.json", matches_path)
    with open(matches_path, "rb") as f:
        match_ids = [m["match_id"] for m in json.load(f)]

    with ThreadPoolExecutor(max_workers=16) as pool:
        written = list(
            pool.map(
                lambda match_id: land(
                    f"{SOURCE}/events/{match_id}.json",
                    f"{RAW}/events/{season}/{match_id}.json",
                ),
                match_ids,
            )
        )

    return {
        "matches": len(match_ids),
        "files_downloaded": sum(1 for b in written if b),
        "megabytes_downloaded": round(sum(written) / 1e6),
        "files_already_landed": sum(1 for b in written if not b),
    }


# ---------------------------------------------------------------- bronze


def bronze_rows(spark: SparkSession, competition_id: int, season_id: int) -> DataFrame:
    """One raw row per event for one landed competition season."""
    # One row per file, the whole file in a single column. basePath keeps
    # competition_id and season_id, read from the folder names, as columns.
    files = spark.read.option("basePath", f"{RAW}/events").text(
        f"{RAW}/events/competition_id={competition_id}/season_id={season_id}",
        wholetext=True,
    )
    return files.select(
        "competition_id",
        "season_id",
        F.regexp_extract(F.col("_metadata.file_name"), r"(\d+)\.json", 1)
        .cast("long")
        .alias("match_id"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("loaded_at"),
        # The file is a list of events. Read it as a list of raw strings, then give
        # each string its own row.
        F.explode(F.from_json("value", "array<string>")).alias("event_json"),
    )


def load_bronze(
    spark: SparkSession, schema: str, competition_id: int, season_id: int
) -> dict:
    """Put one landed season into bronze, replacing that season if it is already there."""
    bronze = f"{schema}.bronze_events"
    season = season_filter(competition_id, season_id)
    (
        bronze_rows(spark, competition_id, season_id)
        .write.mode("overwrite")
        .option("replaceWhere", season)
        .saveAsTable(bronze)
    )
    return {"rows": spark.table(bronze).filter(season).count()}


# ---------------------------------------------------------------- silver


def silver_rows(bronze_df: DataFrame) -> DataFrame:
    """Bronze rows parsed into typed columns, still one row per event."""
    parsed = bronze_df.withColumn(
        "event", F.from_json("event_json", EVENT_SCHEMA, {"mode": "FAILFAST"})
    )
    return parsed.select(
        F.col("event.id").alias("event_id"),
        "competition_id",
        "season_id",
        "match_id",
        F.col("event.index").alias("event_index"),
        F.col("event.period").alias("period"),
        F.col("event.minute").alias("minute"),
        F.col("event.second").alias("second"),
        F.col("event.type.name").alias("event_type"),
        F.col("event.team.id").alias("team_id"),
        F.col("event.team.name").alias("team"),
        # Null for events that belong to a team rather than a player, such as a line-up.
        F.col("event.player.id").alias("player_id"),
        F.col("event.player.name").alias("player"),
        # Pitch position, where the event has one.
        F.get(F.col("event.location"), F.lit(0)).alias("x"),
        F.get(F.col("event.location"), F.lit(1)).alias("y"),
        # Only shots carry these.
        F.col("event.shot.statsbomb_xg").alias("shot_xg"),
        F.col("event.shot.outcome.name").alias("shot_outcome"),
        # A fingerprint of the raw event. If the source record changes, so does this.
        F.sha2("event_json", 256).alias("event_hash"),
    )


def merge_into_silver(
    spark: SparkSession, schema: str, competition_id: int, season_id: int
) -> dict:
    """MERGE one season's bronze rows into silver. Returns the MERGE's row counts."""
    silver = f"{schema}.silver_events"
    season = season_filter(competition_id, season_id)
    new_events = silver_rows(spark.table(f"{schema}.bronze_events").filter(season))

    # First run only: create silver empty, with its columns and its partitioning.
    # Once the table exists this write does nothing.
    (
        new_events.limit(0)
        .write.mode("ignore")
        .partitionBy("competition_id", "season_id")
        .saveAsTable(silver)
    )

    new_events.createOrReplaceTempView("new_events")
    counts = spark.sql(
        f"""
        MERGE INTO {silver} AS s
        USING new_events AS n
        ON s.competition_id = {competition_id}
            AND s.season_id = {season_id}
            AND s.event_id = n.event_id
        WHEN MATCHED AND s.event_hash <> n.event_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    ).first()
    return {
        "rows_inserted": counts["num_inserted_rows"],
        "rows_updated": counts["num_updated_rows"],
    }


def build_matches(spark: SparkSession, schema: str) -> dict:
    """Rebuild the match list (one row per match) from every landed season."""
    matches = f"{schema}.silver_matches"
    (
        spark.read.schema(MATCH_SCHEMA)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(f"{RAW}/matches")
        .select(
            "competition_id",
            "season_id",
            "match_id",
            "match_date",
            "match_week",
            F.col("home_team.home_team_id").alias("home_team_id"),
            F.col("home_team.home_team_name").alias("home_team"),
            F.col("away_team.away_team_id").alias("away_team_id"),
            F.col("away_team.away_team_name").alias("away_team"),
            "home_score",
            "away_score",
        )
        .write.mode("overwrite")
        .saveAsTable(matches)
    )
    return {"rows": spark.table(matches).count()}


# ---------------------------------------------------------------- gold


def build_gold_team_match_xg(spark: SparkSession, schema: str) -> dict:
    """Rebuild one row per team per match from everything currently in silver."""
    gold = f"{schema}.gold_team_match_xg"
    (
        spark.table(f"{schema}.silver_events")
        .groupBy("competition_id", "season_id", "match_id", "team_id", "team")
        .agg(
            F.count(F.when(IS_SHOT, True)).alias("shots"),
            F.count(F.when(IS_GOAL, True)).alias("goals"),
            F.round(F.coalesce(F.sum("shot_xg"), F.lit(0.0)), 2).alias("xg"),
        )
        .write.mode("overwrite")
        .saveAsTable(gold)
    )
    return {"rows": spark.table(gold).count()}


def build_gold_match_xg(spark: SparkSession, schema: str) -> dict:
    """Rebuild one row per match, home and away, with the official score alongside."""
    gold = f"{schema}.gold_match_xg"
    events = spark.table(f"{schema}.silver_events")
    matches = spark.table(f"{schema}.silver_matches")
    at_home = F.col("team_id") == F.col("home_team_id")
    (
        # The match list is tiny against the events, so it is broadcast to every
        # worker and the events are not shuffled for the join.
        events.join(F.broadcast(matches), ["competition_id", "season_id", "match_id"])
        .groupBy(
            "competition_id",
            "season_id",
            "match_id",
            "match_date",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
        )
        .agg(
            F.count(F.when(IS_GOAL & at_home, True)).alias("home_goals"),
            F.count(F.when(IS_GOAL & ~at_home, True)).alias("away_goals"),
            F.round(
                F.coalesce(F.sum(F.when(at_home, F.col("shot_xg"))), F.lit(0.0)), 2
            ).alias("home_xg"),
            F.round(
                F.coalesce(F.sum(F.when(~at_home, F.col("shot_xg"))), F.lit(0.0)), 2
            ).alias("away_xg"),
        )
        .write.mode("overwrite")
        .saveAsTable(gold)
    )
    return {"rows": spark.table(gold).count()}
