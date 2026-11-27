"""SECOP Ledger daily pipeline, as an Airflow DAG.

    heartbeat_start -> collect -> silver -> changes -> dbt_build -> checks -> heartbeat_ok

A THIN DAG: every task calls the same command line as scripts/run_collector.ps1, from the pipeline's
own virtual environment (/opt/secop-venv). The logic, its tests and its alerts live in the
`secop_ledger` package; this file only says order, schedule, retries and timeouts.

Things to know:
- catchup=False. Backfill re-runs old logical dates, but the source keeps no history: a run for
  "2026-11-25" executed today would download TODAY's data. So the collector ignores the logical
  date and uses the real date. Silver, changes and dbt rebuild every missing day by themselves.
- Retries are safe because every step is idempotent: a day is replaced, never appended.
- execution_timeout turns a hang (the 2026-11-25 run hung for hours while the PC slept) into a
  failure, which is retried and alerted.
- on_failure_callback runs only after the LAST retry: a blip fixed by a retry does not wake you up.
"""

from __future__ import annotations

import functools
import logging
import re
import subprocess
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = "/opt/secop"
PY = "/opt/secop-venv/bin/python"
DBT = "/opt/secop-venv/bin/dbt"

log = logging.getLogger(__name__)


def _exit_code(context) -> int:
    # BashOperator fails with "...returned a non-zero exit code N." (a timeout has no code).
    match = re.search(r"exit code (\d+)", str(context.get("exception") or ""))
    return int(match.group(1)) if match else 1


def _secop(*args: str) -> None:
    """Run a secop_ledger command from a callback. Never raises: alerts must not hide errors."""
    try:
        subprocess.run([PY, "-m", *args], cwd=REPO, check=False, timeout=120)
    except Exception:
        log.exception("callback command failed: %s", args)


def on_task_failure(context, notify: bool = True) -> None:
    """After the last retry: phone alert (unless the task alerts by itself) + heartbeat 'failed'."""
    step = context["task_instance"].task_id
    code = str(_exit_code(context))
    if notify:
        _secop("secop_ledger.monitor.notify", "--step", step, "--exit-code", code)
    _secop("secop_ledger.monitor.heartbeat", "exit", code)


def step(task_id: str, command: str, timeout: timedelta, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {REPO} && {command}",
        execution_timeout=timeout,
        **kwargs,
    )


with DAG(
    dag_id="secop_daily",
    description="SECOP II contracts: raw snapshot -> silver -> changes -> dbt gold -> checks",
    schedule="0 7 * * *",  # 07:00 every day (Bogota: see AIRFLOW__CORE__DEFAULT_TIMEZONE)
    # In the past on purpose: Airflow runs no task for a logical date before start_date (a manual
    # run "succeeds" with 0 tasks). catchup=False, so no run is created for the days since then.
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    catchup=False,
    max_active_runs=1,  # never two downloads at the same time
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_task_failure,
    },
    tags=["secop-ledger"],
) as dag:
    heartbeat_start = step("heartbeat_start", f"{PY} -m secop_ledger.monitor.heartbeat start",
                           timedelta(minutes=2), retries=0)

    # The network is the usual reason to fail: wait longer before the retry.
    collect = step("collect", f"{PY} -m secop_ledger.ingest.collect", timedelta(hours=4),
                   retries=2, retry_delay=timedelta(minutes=20))
    silver = step("silver", f"{PY} -m secop_ledger.silver.build", timedelta(minutes=30))
    changes = step("changes", f"{PY} -m secop_ledger.silver.changes", timedelta(minutes=30))
    dbt_build = step("dbt_build", f"{DBT} build --project-dir dbt --profiles-dir dbt",
                     timedelta(minutes=30))

    # The checks send their own notification (silent if OK); a FAIL check exits with 5.
    # No retry: the same data gives the same result.
    checks = step("checks", f"{PY} -m secop_ledger.monitor.checks", timedelta(minutes=5),
                  retries=0,
                  on_failure_callback=functools.partial(on_task_failure, notify=False))

    heartbeat_ok = step("heartbeat_ok", f"{PY} -m secop_ledger.monitor.heartbeat exit 0",
                        timedelta(minutes=2), retries=0)

    heartbeat_start >> collect >> silver >> changes >> dbt_build >> checks >> heartbeat_ok
