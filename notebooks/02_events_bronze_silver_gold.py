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
import urllib.request
from concurrent.futures import ThreadPoolExecutor

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

from pyspark.sql import functions as F

BRONZE = "workspace.rh_lakehouse.bronze_events"

# One row per file, the whole file in a single column. competition_id and season_id
# come from the folder names.
files = spark.read.text(f"{RAW}/events", wholetext=True)

bronze = files.select(
    "competition_id",
    "season_id",
    F.regexp_extract(F.col("_metadata.file_name"), r"(\d+)\.json", 1)
    .cast("long")
    .alias("match_id"),
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("loaded_at"),
    # The file is a list of events. Read it as a list of raw strings, then give each
    # string its own row.
    F.explode(F.from_json("value", "array<string>")).alias("event_json"),
)

bronze.write.mode("overwrite").saveAsTable(BRONZE)

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

parsed = spark.table(BRONZE).withColumn(
    "event", F.from_json("event_json", event_schema, {"mode": "FAILFAST"})
)

silver = parsed.select(
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
)

(
    silver.write.mode("overwrite")
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
