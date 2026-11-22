# Databricks notebook source
# MAGIC %md
# MAGIC # Run one pipeline step
# MAGIC
# MAGIC The entry point the orchestrator calls. It runs one step from `steps.py`, chosen
# MAGIC by the `step` parameter, and hands that step's counts back as the run's output.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `step` | `land`, `bronze`, `silver`, `matches`, `gold_team` or `gold_match` |
# MAGIC | `schema` | Where the tables are written, for example `workspace.rh_lakehouse` |
# MAGIC | `competition_id`, `season_id` | The season to load. Only the first three steps take them. |

# COMMAND ----------

import json

import steps

schema = dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def season() -> tuple[int, int]:
    return (
        int(dbutils.widgets.get("competition_id")),
        int(dbutils.widgets.get("season_id")),
    )


STEPS = {
    "land": lambda: steps.land_season(*season()),
    "bronze": lambda: steps.load_bronze(spark, schema, *season()),
    "silver": lambda: steps.merge_into_silver(spark, schema, *season()),
    "matches": lambda: steps.build_matches(spark, schema),
    "gold_team": lambda: steps.build_gold_team_match_xg(spark, schema),
    "gold_match": lambda: steps.build_gold_match_xg(spark, schema),
}

result = STEPS[dbutils.widgets.get("step")]()
print(result)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))
