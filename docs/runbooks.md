# Runbooks

What to do when an alert arrives. Each alert links to its section here (tap the notification).
Logs: `data/logs/collect-YYYY-MM-DD.log`. Every step is safe to run again: a day is replaced, never
appended, and a failed collector run publishes nothing.

The daily task runs: `collect → silver → changes → dbt build → checks`. Each step runs only if the
previous one worked.

## step-failed

**Alert:** `SECOP Ledger: <step> FAILED` (exit code ≠ 0).

1. Read the last lines in the alert, then the full log of the day.
2. Network or API errors (`ConnectionError`, `Timeout`, HTTP 5xx) are usually temporary.
   Run the whole day again: `powershell -File scripts/run_collector.ps1`. Steps that already
   finished do nothing the second time.
3. A `silver` failure for an unknown status: add the new value to `status_map.csv`, then run again
   (raw keeps the day, nothing is lost).
4. A `dbt` failure: run `uv run dbt build --project-dir dbt --profiles-dir dbt` and read which test
   failed. A failed reconciliation test means the layers do not agree: do not delete anything.

## schema-drift

**Alert:** `collect FAILED`, exit code 1. The source dropped or renamed a column we need.
Nothing was downloaded.

1. The log lists the missing columns. Check the dataset page on datos.gov.co.
2. Renamed column → update `SELECTED_COLUMNS` and the silver mapping, with a test. Removed column →
   decide if silver can live without it. This needs a code change and a PR, not a re-run.

## count-mismatch

**Alert:** `collect FAILED`, exit code 2. Rows written ≠ the source `count(*)` (> 0.5 %).
Nothing was published.

1. More rows than the source: rows were read twice (see the 2026-11-25 learning: the source
   reloaded during the read). Fewer rows: pages were lost.
2. Run the collector again. If it fails twice in a row, it is a bug: compare the `:id` ranges.

## source-reloaded-during-the-download

**Alert:** `collect FAILED`, exit code 4. The source reloaded during both tries (every reload
gives every row a new id, so the read would mix two versions).

1. Rare: the source reloads about once a day. Wait 1 hour and run the collector again.
2. If it happens every day, the source changed its reload schedule: move the task time.

## missed-day

**Alert:** `checks WARN`, "N day(s) missing". A day has no snapshot (PC off, or the run failed).

1. The day can't be recovered: the source keeps no history. Don't create fake data.
2. Nothing to repair: change detection compares with the last snapshot and records `gap_days`.
3. Find out why (PC off? task disabled? `Get-ScheduledTaskInfo "SECOP Ledger collector"`).

## source-not-refreshed

**Alert:** `checks WARN`, "the source did not reload". `rowsUpdatedAt` is the same as the
previous run, so today's snapshot repeats the last one and has 0 changes.

1. Usually the source just skipped a day (it happened on 2026-11-22). Nothing to do.
2. If it lasts 3+ days, check the dataset page: the publisher may have stopped updating it.

## row-count-drop

**Alert:** `checks FAIL` (exit code 5), the slice lost more than 1 % of its rows since the previous run.
The slice only grows, so this is suspicious. The data is already published.

1. Compare with the source: `count(*)` for the slice on the API (see the snippet in the vault).
2. If the source really dropped rows, it shows in `contract_changes` as `disappeared`: check
   them before using the gold numbers of that day.
3. If the source did not drop them, the download lost pages: run the day again with
   `uv run python -m secop_ledger.ingest.collect --force`, then the rest of the steps.

## slow-run

**Alert:** `checks WARN`, the download took more than 2× the median of the last runs.

1. Look at the timestamps between pages in the log. A gap of hours = the PC slept or the network
   dropped; many slow pages = the API was slow.
2. Nothing to repair if the checks are OK otherwise. If it repeats, consider a Socrata app token.

## heartbeat-down

**Alert:** from healthchecks.io (email or ntfy): the check "SECOP Ledger daily" is **down**.
No success ping arrived by the deadline (07:00 Bogotá + 8 h grace), or the run reported a
non-zero exit code. This is the only alert that works when the PC is off.

1. Is the PC on? If it was off all day, the day is lost (see [missed-day](#missed-day)).
2. The PC is on but no ping: is the task enabled and did it start?
   `Get-ScheduledTaskInfo "SECOP Ledger collector"` (`LastRunTime`, `LastTaskResult`).
3. A `start` ping but no end ping: the run hung or was killed. Read the day's log: the last line
   shows the step. Run it again: `powershell -File scripts/run_collector.ps1`.
4. An end ping with a non-zero code: the ntfy alert for that step came first; follow its runbook.
   healthchecks.io shows the last 30 log lines in the ping's body.
