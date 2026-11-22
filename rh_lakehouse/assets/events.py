"""The lakehouse tables as Dagster assets. Every asset is one step of
pipeline/steps.py, run on Databricks. The first three are partitioned by
competition season, so a new season costs one season of work: its files are
landed, swapped into bronze and merged into silver, and nothing already loaded
is parsed again."""
import dagster as dg

from ..resources import DatabricksResource

# Partition key -> (competition_id, season_id) in the source data.
SEASONS = {
    "premier_league_2015_16": (2, 27),
    "la_liga_2015_16": (11, 27),
    "serie_a_2015_16": (12, 27),
    "ligue_1_2015_16": (7, 27),
}
SEASON_PARTITIONS = dg.StaticPartitionsDefinition(list(SEASONS))


def run_step(
    databricks: DatabricksResource, step: str, partition_key: str | None = None
) -> dg.MaterializeResult:
    """Run one step on Databricks and record its counts against the asset."""
    parameters = {}
    if partition_key:
        competition_id, season_id = SEASONS[partition_key]
        parameters = {"competition_id": competition_id, "season_id": season_id}
    counts, run_url = databricks.run_step(step, **parameters)
    return dg.MaterializeResult(
        metadata={**counts, "databricks_run": dg.MetadataValue.url(run_url)}
    )


@dg.asset(group_name="landing", partitions_def=SEASON_PARTITIONS)
def raw_events(
    context: dg.AssetExecutionContext, databricks: DatabricksResource
) -> dg.MaterializeResult:
    """One season's source files in the volume, unchanged: the match list and one
    JSON file of events per match. A file already landed is skipped."""
    return run_step(databricks, "land", context.partition_key)


@dg.asset(group_name="bronze", partitions_def=SEASON_PARTITIONS, deps=[raw_events])
def bronze_events(
    context: dg.AssetExecutionContext, databricks: DatabricksResource
) -> dg.MaterializeResult:
    """One raw row per event. The season is swapped in with replaceWhere, so
    loading it twice cannot double it."""
    return run_step(databricks, "bronze", context.partition_key)


@dg.asset(group_name="silver", partitions_def=SEASON_PARTITIONS, deps=[bronze_events])
def silver_events(
    context: dg.AssetExecutionContext, databricks: DatabricksResource
) -> dg.MaterializeResult:
    """Typed columns, one row per event, partitioned by competition and season.
    The season is merged in on event_id: new events are inserted, changed ones
    updated, unchanged ones left alone."""
    return run_step(databricks, "silver", context.partition_key)


@dg.asset(group_name="silver", deps=[raw_events])
def silver_matches(databricks: DatabricksResource) -> dg.MaterializeResult:
    """One row per match from every landed season: teams, date, official score."""
    return run_step(databricks, "matches")


@dg.asset(group_name="gold", deps=[silver_events])
def gold_team_match_xg(databricks: DatabricksResource) -> dg.MaterializeResult:
    """Shots, goals and xG per team per match, rebuilt from silver."""
    return run_step(databricks, "gold_team")


@dg.asset(group_name="gold", deps=[silver_events, silver_matches])
def gold_match_xg(databricks: DatabricksResource) -> dg.MaterializeResult:
    """One row per match with each side's goals and xG. Events are joined to the
    match list as a broadcast join."""
    return run_step(databricks, "gold_match")
