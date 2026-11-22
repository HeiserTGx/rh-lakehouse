import dagster as dg

from .assets import checks, events
from .resources import DatabricksResource

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([events]),
    asset_checks=[checks.event_ids_unique, checks.goals_match_official_scores],
    resources={
        "databricks": DatabricksResource(
            host=dg.EnvVar("DATABRICKS_HOST"),
            token=dg.EnvVar("DATABRICKS_TOKEN"),
            warehouse_id=dg.EnvVar("DATABRICKS_WAREHOUSE_ID"),
            target_schema=dg.EnvVar("LAKEHOUSE_SCHEMA"),
            git_branch=dg.EnvVar("LAKEHOUSE_GIT_BRANCH"),
        ),
    },
)
