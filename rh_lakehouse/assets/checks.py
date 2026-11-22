"""Data-quality checks, run as SQL on the Databricks SQL warehouse. A check
failing means the build worked but the data breaks a rule; an asset failing
means the build itself broke."""
import dagster as dg

from ..resources import DatabricksResource
from .events import gold_match_xg, silver_events


@dg.asset_check(
    asset=silver_events,
    description="No event is in silver twice. This is what the MERGE on event_id promises.",
)
def event_ids_unique(databricks: DatabricksResource) -> dg.AssetCheckResult:
    events, distinct_ids = databricks.query(
        "select count(*), count(distinct event_id) "
        f"from {databricks.target_schema}.silver_events"
    )[0]
    duplicated = int(events) - int(distinct_ids)
    return dg.AssetCheckResult(
        passed=duplicated == 0,
        metadata={"events": int(events), "duplicated": duplicated},
    )


@dg.asset_check(
    asset=gold_match_xg,
    description="Goals counted from events equal the official score in every match.",
)
def goals_match_official_scores(databricks: DatabricksResource) -> dg.AssetCheckResult:
    matches, mismatches = databricks.query(
        "select count(*), "
        "count_if(home_goals <> home_score or away_goals <> away_score) "
        f"from {databricks.target_schema}.gold_match_xg"
    )[0]
    return dg.AssetCheckResult(
        passed=int(mismatches) == 0,
        metadata={"matches": int(matches), "mismatches": int(mismatches)},
    )
