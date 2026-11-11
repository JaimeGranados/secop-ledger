# Daily pipeline, for Windows Task Scheduler: collect raw, then build silver.
# Runs from the repo root and appends to one log file per day in data/logs/.
Set-Location (Split-Path -Parent $PSScriptRoot)

$logDir = "data/logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir ("collect-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

# cmd /c does the redirection: Windows PowerShell 5.1 turns stderr lines of a program into
# errors, and Python's logging writes to stderr.
cmd /c "uv run python -m secop_ledger.ingest.collect >> `"$log`" 2>&1"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Silver only runs if the collector worked. It builds every raw day that has no silver yet.
cmd /c "uv run python -m secop_ledger.silver.build >> `"$log`" 2>&1"
exit $LASTEXITCODE
