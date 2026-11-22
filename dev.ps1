# Start the local Dagster dev server.
# - loads .env so the Databricks resource can resolve its env vars
# - sets a persistent DAGSTER_HOME so materialisation history survives restarts
$root = $PSScriptRoot

Get-Content (Join-Path $root ".env") | ForEach-Object {
    $k, $v = $_ -split "=", 2
    if ($k) { Set-Item "env:$k" $v }
}

$env:DAGSTER_HOME = Join-Path $root "dagster_home"

& (Join-Path $root ".venv\Scripts\Activate.ps1")

dagster dev
