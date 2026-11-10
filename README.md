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

## Data

One row per finished match in Europe's top five leagues from 2014/15 onwards (about
21.6k matches): teams, goals and expected goals, sourced from Understat by the ingestion
asset in rh-analytics-pipeline. The file is not committed here. To reproduce, run that
asset's backfill, export `understat_match_xg` to CSV and upload it to the
`workspace.rh_lakehouse.raw` volume.
