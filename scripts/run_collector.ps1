# Daily pipeline, for Windows Task Scheduler: collect raw -> silver -> changes -> dbt gold -> checks.
# Runs from the repo root and appends to one log file per day in data/logs/.
# A failed step sends an alert (ntfy: phone push + email) and stops the run with that step's code.
Set-Location (Split-Path -Parent $PSScriptRoot)

$logDir = "data/logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir ("collect-{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

function Invoke-Step($name, $command) {
    # cmd /c does the redirection: Windows PowerShell 5.1 turns stderr lines of a program into
    # errors, and Python's logging writes to stderr.
    cmd /c "$command >> `"$log`" 2>&1"
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        cmd /c "uv run python -m secop_ledger.monitor.notify --step $name --exit-code $code --log `"$log`" >> `"$log`" 2>&1"
        exit $code
    }
}

# Each step only runs if the previous one worked. Each builds every day that is still missing.
Invoke-Step "collect" "uv run python -m secop_ledger.ingest.collect"
Invoke-Step "silver" "uv run python -m secop_ledger.silver.build"
Invoke-Step "changes" "uv run python -m secop_ledger.silver.changes"
# Gold: rebuild the dbt models and run every dbt test (a failed test fails the task).
Invoke-Step "dbt" "uv run dbt build --project-dir dbt --profiles-dir dbt"

# Data checks: they send their own notification (silent if all OK). Exit code 5 = a FAIL check.
cmd /c "uv run python -m secop_ledger.monitor.checks >> `"$log`" 2>&1"
exit $LASTEXITCODE
