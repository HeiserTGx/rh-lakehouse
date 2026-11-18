# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Match events: raw JSON to bronze, silver and gold
# MAGIC
# MAGIC Notebook 01 started from a table that arrives already summarised: one row per match
# MAGIC with each side's xG. This notebook starts a level below that, from the events inside
# MAGIC each match (every pass, carry, pressure and shot), and builds the match-level table
# MAGIC from them.
# MAGIC
# MAGIC The source is public match event data: one JSON file per match, a few thousand
# MAGIC events each. It is loaded one competition season at a time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Land the raw files
# MAGIC
# MAGIC The files are downloaded straight into the volume, unchanged. Each competition
# MAGIC season gets its own folder, named `competition_id=…/season_id=…`. Spark reads folder
# MAGIC names written this way as columns, so every row will know which season it came from
# MAGIC without that being stored inside the file.
# MAGIC
# MAGIC A file that is already in the volume is skipped, so the cell can be run again safely.

# COMMAND ----------

import json
import os
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F

SOURCE = "https://raw.githubusercontent.com/hudl/open-data/master/data"
RAW = "/Volumes/workspace/rh_lakehouse/raw"


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def land(url: str, path: str) -> int:
    """Download one file into the volume. Returns bytes written, 0 if already there."""
    if os.path.exists(path):
        return 0
    body = fetch(url)
    with open(path, "wb") as f:
        f.write(body)
    return len(body)


def land_season(competition_id: int, season_id: int) -> None:
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

    print(
        f"{season}: {len(match_ids)} matches, "
        f"{sum(1 for b in written if b)} files downloaded "
        f"({sum(written) / 1e6:,.0f} MB), "
        f"{sum(1 for b in written if not b)} already in the volume"
    )


# Premier League 2015/16
land_season(competition_id=2, season_id=27)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bronze: one row per event, still raw
# MAGIC
# MAGIC Each file is one match: a single JSON list holding a few thousand events. Bronze
# MAGIC splits that list into rows and stops there. The event itself is kept as the original
# MAGIC text, untyped and uncleaned, with three things added that the file does not carry:
# MAGIC the match it belongs to, the file it came from and when it was loaded.
# MAGIC
# MAGIC Keeping bronze raw means a mistake in the cleaning step later can be fixed by
# MAGIC re-running from this table, without downloading anything again.

# COMMAND ----------

BRONZE = "workspace.rh_lakehouse.bronze_events"


def bronze_rows(competition_id: int, season_id: int):
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


# First load: the table is created from the Premier League season alone.
bronze_rows(competition_id=2, season_id=27).write.mode("overwrite").saveAsTable(BRONZE)

# COMMAND ----------

bronze_events = spark.table(BRONZE)

print("matches:", bronze_events.select("match_id").distinct().count())
print("bronze rows:", bronze_events.count())
display(bronze_events.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Silver: typed columns, one row per event
# MAGIC
# MAGIC Silver reads the raw text in bronze and pulls the useful fields into real columns
# MAGIC with real types. The schema lists only the fields silver needs; everything else an
# MAGIC event carries stays in bronze and can be added later without a new download.
# MAGIC
# MAGIC As in notebook 01 the parse uses `FAILFAST`, so an event that does not fit the
# MAGIC schema stops the run instead of quietly becoming a row of nulls.
# MAGIC
# MAGIC Each row also carries a hash of the raw event text. It is used further down to tell
# MAGIC whether an event that is already in the table has changed at the source.
# MAGIC
# MAGIC The table is stored partitioned by competition and season: each season's rows are
# MAGIC written to their own folder.

# COMMAND ----------

SILVER = "workspace.rh_lakehouse.silver_events"

event_schema = """
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


def silver_rows(bronze_df):
    """Bronze rows parsed into typed columns, still one row per event."""
    parsed = bronze_df.withColumn(
        "event", F.from_json("event_json", event_schema, {"mode": "FAILFAST"})
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


(
    silver_rows(spark.table(BRONZE))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("competition_id", "season_id")
    .saveAsTable(SILVER)
)

# COMMAND ----------

silver_events = spark.table(SILVER)

print("silver rows:", silver_events.count())
display(silver_events.groupBy("event_type").count().orderBy(F.desc("count")).limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC The first shots of one match (Chelsea v Stoke City), now as ordinary columns.

# COMMAND ----------

display(
    silver_events.filter((F.col("match_id") == 3754061) & (F.col("event_type") == "Shot"))
    .orderBy("event_index")
    .select("minute", "team", "player", "x", "y", "shot_xg", "shot_outcome")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Gold: one row per team per match
# MAGIC
# MAGIC Gold is the table someone would actually query. It collapses the events to one row
# MAGIC per team per match: shots, goals and total xG. That is the same grain notebook 01
# MAGIC reshaped the match table to, except here it is built up from the events themselves.
# MAGIC
# MAGIC A goal is a shot whose outcome is `Goal`, or an own goal credited to the team.

# COMMAND ----------

GOLD = "workspace.rh_lakehouse.gold_team_match_xg"

is_shot = F.col("event_type") == "Shot"
is_goal = (F.col("shot_outcome") == "Goal") | (F.col("event_type") == "Own Goal For")


def build_gold() -> None:
    """Rebuild the gold table from everything currently in silver."""
    (
        spark.table(SILVER)
        .groupBy("competition_id", "season_id", "match_id", "team_id", "team")
        .agg(
            F.count(F.when(is_shot, True)).alias("shots"),
            F.count(F.when(is_goal, True)).alias("goals"),
            F.round(F.coalesce(F.sum("shot_xg"), F.lit(0.0)), 2).alias("xg"),
        )
        .write.mode("overwrite")
        .saveAsTable(GOLD)
    )


build_gold()

# COMMAND ----------

gold_xg = spark.table(GOLD)

print("gold rows:", gold_xg.count())
display(gold_xg.filter(F.col("match_id") == 3754061))

# COMMAND ----------

# MAGIC %md
# MAGIC Season totals from the gold table, as a check against the real 2015/16 league table.

# COMMAND ----------

display(
    gold_xg.groupBy("team")
    .agg(
        F.count("*").alias("played"),
        F.sum("goals").alias("goals"),
        F.round(F.sum("xg"), 1).alias("xg"),
    )
    .orderBy(F.desc("goals"))
    .limit(6)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · A new season arrives: MERGE
# MAGIC
# MAGIC Everything above built the tables from scratch. That is fine once. It is the wrong
# MAGIC way to add data: rebuilding silver for every new season means re-parsing every old
# MAGIC season too.
# MAGIC
# MAGIC Here a second season lands (La Liga 2015/16) and only that season is processed.
# MAGIC
# MAGIC Bronze first. `replaceWhere` swaps out one season's rows and leaves the rest of the
# MAGIC table alone, so loading the same season twice cannot double it.

# COMMAND ----------

def load_bronze(competition_id: int, season_id: int) -> None:
    """Put one landed season into bronze, replacing that season if it is already there."""
    (
        bronze_rows(competition_id, season_id)
        .write.mode("overwrite")
        .option(
            "replaceWhere",
            f"competition_id = {competition_id} AND season_id = {season_id}",
        )
        .saveAsTable(BRONZE)
    )


# La Liga 2015/16
land_season(competition_id=11, season_id=27)
load_bronze(competition_id=11, season_id=27)

display(spark.table(BRONZE).groupBy("competition_id", "season_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Now silver. `MERGE` compares the incoming events with the table on `event_id`:
# MAGIC
# MAGIC - an event that is not there is inserted
# MAGIC - an event that is already there is updated, but only if its hash differs, meaning
# MAGIC   the source record has changed since it was loaded
# MAGIC - an event that is already there and unchanged is left alone
# MAGIC
# MAGIC The match condition also names the season being loaded. Silver is partitioned by
# MAGIC competition and season, so that lets Delta go straight to that season's files and
# MAGIC leave the Premier League ones alone.
# MAGIC
# MAGIC The result of a MERGE is a one-row table saying how many rows it inserted and
# MAGIC updated.

# COMMAND ----------

def merge_into_silver(competition_id: int, season_id: int):
    """MERGE one season's bronze rows into silver. Returns the MERGE's row counts."""
    season = f"competition_id = {competition_id} AND season_id = {season_id}"
    silver_rows(spark.table(BRONZE).filter(season)).createOrReplaceTempView("new_events")
    return spark.sql(
        f"""
        MERGE INTO {SILVER} AS s
        USING new_events AS n
        ON s.competition_id = {competition_id}
            AND s.season_id = {season_id}
            AND s.event_id = n.event_id
        WHEN MATCHED AND s.event_hash <> n.event_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


print("silver rows before:", spark.table(SILVER).count())
display(merge_into_silver(competition_id=11, season_id=27))
print("silver rows after:", spark.table(SILVER).count())

# COMMAND ----------

# MAGIC %md
# MAGIC The same MERGE again, with the same season. A plain append would add every row a
# MAGIC second time. MERGE finds each event already in the table, so nothing is inserted.
# MAGIC No hash differs, so nothing is updated either: the run changes no rows at all.

# COMMAND ----------

display(merge_into_silver(competition_id=11, season_id=27))
print("silver rows after a second run:", spark.table(SILVER).count())

# COMMAND ----------

# MAGIC %md
# MAGIC Gold is small, so it is simply rebuilt from silver.

# COMMAND ----------

build_gold()

display(
    spark.table(GOLD)
    .groupBy("competition_id", "season_id")
    .agg(F.count("*").alias("team_match_rows"), F.sum("goals").alias("goals"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Joining a big table to a small one: the broadcast join
# MAGIC
# MAGIC The events say which team did something, not whether that team was at home. That
# MAGIC lives in the match list landed next to the events: one row per match, a few hundred
# MAGIC rows against a few million events.

# COMMAND ----------

MATCHES = "workspace.rh_lakehouse.silver_matches"

match_schema = """
    match_id long,
    match_date date,
    match_week int,
    home_team struct<home_team_id: int, home_team_name: string>,
    away_team struct<away_team_id: int, away_team_name: string>,
    home_score int,
    away_score int
"""

(
    spark.read.schema(match_schema)
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
    .saveAsTable(MATCHES)
)

events = spark.table(SILVER)
matches = spark.table(MATCHES)

print("events:", events.count())
print("matches:", matches.count())

# COMMAND ----------

# MAGIC %md
# MAGIC A join needs rows with the same `match_id` in the same place. The general way to
# MAGIC get there is to shuffle both tables by the join key, which here means moving every
# MAGIC one of the events. The `merge` hint asks Spark for that plan so it can be seen.

# COMMAND ----------

join_keys = ["competition_id", "season_id", "match_id"]

events.join(matches.hint("merge"), join_keys).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC The match list is tiny. Instead of moving the events to meet it, Spark can send a
# MAGIC full copy of the match list to every worker and leave the events where they are.
# MAGIC That is a broadcast join: the small table is broadcast, the big table is not
# MAGIC shuffled at all.
# MAGIC
# MAGIC Spark picks this on its own when it can tell a table is small. `F.broadcast` states
# MAGIC it outright.

# COMMAND ----------

events_with_match = events.join(F.broadcast(matches), join_keys)

events_with_match.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC With home and away known, the events collapse to one row per match with each side's
# MAGIC goals and xG. This is the shape of the match table notebook 01 started from, built
# MAGIC here from the events. The official score from the match list is kept alongside as
# MAGIC a check on the goals counted from events.

# COMMAND ----------

GOLD_MATCH = "workspace.rh_lakehouse.gold_match_xg"

at_home = F.col("team_id") == F.col("home_team_id")

(
    events_with_match.groupBy(
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
        F.count(F.when(is_goal & at_home, True)).alias("home_goals"),
        F.count(F.when(is_goal & ~at_home, True)).alias("away_goals"),
        F.round(F.coalesce(F.sum(F.when(at_home, F.col("shot_xg"))), F.lit(0.0)), 2).alias("home_xg"),
        F.round(F.coalesce(F.sum(F.when(~at_home, F.col("shot_xg"))), F.lit(0.0)), 2).alias("away_xg"),
    )
    .write.mode("overwrite")
    .saveAsTable(GOLD_MATCH)
)

gold_match = spark.table(GOLD_MATCH)

print("matches:", gold_match.count())
print(
    "matches where goals from events differ from the official score:",
    gold_match.filter(
        (F.col("home_goals") != F.col("home_score"))
        | (F.col("away_goals") != F.col("away_score"))
    ).count(),
)
display(
    gold_match.filter(F.col("competition_id") == 2)
    .orderBy("match_date", "match_id")
    .select("match_date", "home_team", "away_team", "home_goals", "away_goals", "home_xg", "away_xg")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Partitioning: reading only what a query needs
# MAGIC
# MAGIC Silver was written with `partitionBy("competition_id", "season_id")`, so each
# MAGIC season's rows sit in their own folder, in their own files.

# COMMAND ----------

display(
    spark.table(SILVER)
    .groupBy("competition_id", "season_id")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.col("_metadata.file_path")).alias("files"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC A filter on a partition column is answered from the folder names. Spark drops the
# MAGIC folders that cannot match before it opens a single file, which is called partition
# MAGIC pruning. A filter on any other column has no folder to go by: every season is a
# MAGIC candidate and the filter is applied to the data inside the files.
# MAGIC
# MAGIC The plan shows which is which. `PartitionFilters` are applied to folders,
# MAGIC `DataFilters` to rows.

# COMMAND ----------


def scan_filters(where: str) -> None:
    """Print how Spark applies a filter when it scans silver."""
    plan = spark.sql(
        f"EXPLAIN SELECT sum(shot_xg) FROM {SILVER} WHERE {where}"
    ).first()[0]
    print(where)
    for kind, filters in re.findall(r"(PartitionFilters|DataFilters): (\[.*?\])", plan):
        print(f"    {kind}: {filters}")


scan_filters("competition_id = 11")
scan_filters("team = 'Barcelona'")

# COMMAND ----------

# MAGIC %md
# MAGIC A table this size does not need partitioning. Databricks' own guidance is not to
# MAGIC partition tables under 1 TB and to use liquid clustering instead. It is partitioned
# MAGIC here to show how pruning works, which is also what the MERGE in section 4 relies
# MAGIC on when it names the season it is loading.
