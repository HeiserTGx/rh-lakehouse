# Start the local Dagster dev server. Run it from the repo root.
# - dagster dev reads .env from the folder it starts in, so nothing is loaded here
# - sets a persistent DAGSTER_HOME so materialisation history survives restarts
$root = $PSScriptRoot

$env:DAGSTER_HOME = Join-Path $root "dagster_home"

& (Join-Path $root ".venv\Scripts\Activate.ps1")

dagster dev
