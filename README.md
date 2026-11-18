# rh-lakehouse

A PySpark and Databricks version of the historical match-data layer behind
[RH Sports Analysis](https://rhsportsanalysis.com). The production pipeline
([rh-analytics-pipeline](https://github.com/HeiserTGx/rh-analytics-pipeline)) runs on
Postgres, dbt and Dagster, which is the right size for the data. This repo rebuilds the
same layer on Spark to work through how the lakehouse approach differs.

Work in progress. Built on Databricks Free Edition (serverless).

## Notebooks

| Notebook | What it does |
|---|---|
| [`01_match_xg_team_form`](notebooks/01_match_xg_team_form.py) | Reads the raw match CSV with an explicit schema, reshapes it to one row per team per match, builds a season table, and computes rolling five-match xG form with a window function. Ends with the execution plan. |
| [`02_events_bronze_silver_gold`](notebooks/02_events_bronze_silver_gold.py) | Starts a level below notebook 01, from the events inside each match. Lands raw JSON files in a volume, then builds bronze (one raw row per event), silver (typed columns from an explicit schema, partitioned by competition and season) and gold (xG per team per match, and per match with home and away). New seasons are added incrementally: a `replaceWhere` load into bronze, then a `MERGE` into silver that names the partition it is loading and skips unchanged rows by hash. Events join to the match list as a broadcast join, with the shuffle plan shown beside it for comparison. |

## What notebook 02 runs on

| | |
|---|---|
| Matches | 1,517 |
| Events in silver | 5,321,459 |
| Repeat load of a season already in silver | 0 rows inserted, 0 updated |
| Matches where goals counted from events differ from the official score | 0 |

## Data

**Match xG (notebook 01).** One row per finished match in Europe's top five leagues from
2014/15 onwards (about 21.6k matches): teams, goals and expected goals, sourced from
Understat by the ingestion asset in rh-analytics-pipeline. The file is not committed
here. To reproduce, run that asset's backfill, export `understat_match_xg` to CSV and
upload it to the `workspace.rh_lakehouse.raw` volume.

**Match events (notebook 02).** Public match event data for the 2015/16 season of four
leagues (Premier League, La Liga, Serie A, Ligue 1): one JSON file per match, about
3,500 events each. The notebook downloads the files into the same volume itself, so it
runs top to bottom with nothing to upload. No data is committed here.
