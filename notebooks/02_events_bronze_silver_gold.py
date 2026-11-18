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
