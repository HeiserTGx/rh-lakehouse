# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Match xG: raw CSV to team form
# MAGIC
# MAGIC Source: one row per finished match in Europe's top five leagues since 2014/15
# MAGIC (the `understat_match_xg` table from
# MAGIC [rh-analytics-pipeline](https://github.com/HeiserTGx/rh-analytics-pipeline)), exported to CSV.
# MAGIC
# MAGIC This notebook reads the raw file with an explicit schema, reshapes it to one row per
# MAGIC team per match, builds a season table, and computes each team's rolling xG form
# MAGIC going into every match.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · A place for the raw file
# MAGIC
# MAGIC A schema to hold this project, and a volume inside it. A volume is governed file
# MAGIC storage: it holds files as they arrived, before they become tables.
# MAGIC After this cell runs, upload `understat_match_xg.csv` to the volume.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.rh_lakehouse")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.rh_lakehouse.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Read the CSV with an explicit schema
# MAGIC
# MAGIC A CSV carries no types. Spark can guess them (`inferSchema`), but guessing means an
# MAGIC extra pass over the file and a schema that can change when the data does. Declaring
# MAGIC the schema makes the read cheap and the types fixed.
# MAGIC
# MAGIC Two things the schema does not do on its own. By default a value that does not fit
# MAGIC its type is read as null rather than raising, so the read uses `FAILFAST` to stop on
# MAGIC a bad row instead. And the CSV reader ignores `nullable=False`: `printSchema` shows
# MAGIC every column as nullable, because a text file cannot guarantee a value is present.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

RAW_PATH = "/Volumes/workspace/rh_lakehouse/raw/understat_match_xg.csv"

match_schema = T.StructType(
    [
        T.StructField("understat_match_id", T.LongType(), nullable=False),
        T.StructField("kick_off", T.TimestampType(), nullable=False),
        T.StructField("season_label", T.StringType(), nullable=False),
        T.StructField("league", T.StringType(), nullable=False),
        T.StructField("home_team", T.StringType(), nullable=False),
        T.StructField("away_team", T.StringType(), nullable=False),
        T.StructField("home_goals", T.IntegerType(), nullable=False),
        T.StructField("away_goals", T.IntegerType(), nullable=False),
        T.StructField("home_xg", T.DoubleType(), nullable=False),
        T.StructField("away_xg", T.DoubleType(), nullable=False),
    ]
)

matches = spark.read.csv(RAW_PATH, header=True, schema=match_schema, mode="FAILFAST")

matches.printSchema()
print("matches:", matches.count())
display(matches.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · One row per team per match
# MAGIC
# MAGIC A match row has a home side and an away side. Team-level questions ("how has this
# MAGIC team been doing?") need one row per team per match, so each match becomes two rows.
# MAGIC
# MAGIC Nothing in the next cell touches the data. `select` and `unionByName` are
# MAGIC transformations: Spark only records what it has been asked to do.

# COMMAND ----------


def team_view(venue: str):
    """The match table seen from one side: 'home' or 'away'."""
    this, other = ("home", "away") if venue == "home" else ("away", "home")
    return matches.select(
        "understat_match_id",
        "kick_off",
        "season_label",
        "league",
        F.col(f"{this}_team").alias("team"),
        F.col(f"{other}_team").alias("opponent"),
        F.lit(venue).alias("venue"),
        F.col(f"{this}_goals").alias("goals_for"),
        F.col(f"{other}_goals").alias("goals_against"),
        F.col(f"{this}_xg").alias("xg_for"),
        F.col(f"{other}_xg").alias("xg_against"),
    )


team_matches = team_view("home").unionByName(team_view("away"))

# COMMAND ----------

# MAGIC %md
# MAGIC `count()` is an action: this is the moment Spark actually reads the file and does the work.

# COMMAND ----------

print("team-match rows:", team_matches.count())
display(team_matches.orderBy("kick_off").limit(6))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Season table: goals and xG per team per season
# MAGIC
# MAGIC `groupBy` needs every row for the same team and season in the same place, so Spark
# MAGIC moves rows between partitions to bring them together. That move is a shuffle, and it
# MAGIC is usually the expensive part of a Spark job.

# COMMAND ----------

points = (
    F.when(F.col("goals_for") > F.col("goals_against"), 3)
    .when(F.col("goals_for") == F.col("goals_against"), 1)
    .otherwise(0)
)

season_table = (
    team_matches.withColumn("points", points)
    .groupBy("league", "season_label", "team")
    .agg(
        F.count("*").alias("played"),
        F.sum("points").alias("points"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against"),
        F.round(F.sum("xg_for"), 1).alias("xg_for"),
        F.round(F.sum("xg_against"), 1).alias("xg_against"),
    )
    .withColumn("xg_diff", F.round(F.col("xg_for") - F.col("xg_against"), 1))
)

display(
    season_table.filter((F.col("league") == "epl") & (F.col("season_label") == "2025_26"))
    .orderBy(F.desc("points"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Rolling form: average xG over the previous five matches
# MAGIC
# MAGIC A window function keeps every row and adds a value computed from its neighbours.
# MAGIC The window here is "this team's matches in date order, the five before this one".
# MAGIC The current match is left out on purpose: form going into a match must not include
# MAGIC the match itself.

# COMMAND ----------

previous_five = (
    Window.partitionBy("league", "team").orderBy("kick_off").rowsBetween(-5, -1)
)

team_form = team_matches.select(
    "*",
    F.count("*").over(previous_five).alias("matches_in_window"),
    F.round(F.avg("xg_for").over(previous_five), 2).alias("xg_for_last5"),
    F.round(F.avg("xg_against").over(previous_five), 2).alias("xg_against_last5"),
)

display(
    team_form.filter(F.col("team") == "Arsenal")
    .orderBy(F.desc("kick_off"))
    .select(
        "kick_off", "opponent", "venue", "goals_for", "goals_against",
        "xg_for", "matches_in_window", "xg_for_last5", "xg_against_last5",
    )
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · The execution plan
# MAGIC
# MAGIC `explain` shows what Spark will actually do for `team_form`. Read it bottom up:
# MAGIC two scans of the CSV (home view and away view), a union, an exchange (the shuffle
# MAGIC that brings each team's rows together), a sort by kick-off, then the window.

# COMMAND ----------

team_form.explain(mode="formatted")
