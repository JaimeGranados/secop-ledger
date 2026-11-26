"""Data checks after a successful run, and one notification with the result.

A green pipeline is not the same as good data: every step can exit 0 while the source sent nothing
new, lost rows, or the run took 3 times longer. These checks compare today's collector manifest
with the previous ones (no DuckDB: they read a few small JSON files, so they take < 1 s).

    status  meaning                                   notification
    OK      nothing to say                            silent daily summary (priority min)
    WARN    look at it today, data is still usable    push + email (priority high)
    FAIL    the data of the day is suspicious         push + email (urgent), exit code 5

Run:  uv run python -m secop_ledger.monitor.checks [--date YYYY-MM-DD] [--no-notify]
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from secop_ledger.config import Settings
from secop_ledger.monitor import notify

log = logging.getLogger("secop_ledger.checks")

ROW_DROP_FAIL = 0.01  # the slice only grows (new contracts); losing > 1 % of rows is not normal
SLOW_FACTOR = 2.0  # slower than 2x the median of the last runs
SLOW_MIN_RUNS = 3  # need some history before calling a run "slow"
SLOW_WINDOW = 7

EXIT_CHECK_FAILED = 5


@dataclass(frozen=True)
class Check:
    name: str
    status: str  # OK, WARN, FAIL
    detail: str
    runbook: str | None = None


def load_manifests(data_dir: Path) -> dict[date, dict[str, Any]]:
    """Published days only: a `.tmp` folder is a run that never finished."""
    manifests = {}
    for path in (data_dir / "raw" / "contracts").glob("ingest_date=*[0-9]/_manifest.json"):
        m = json.loads(path.read_text(encoding="utf-8"))
        manifests[date.fromisoformat(m["ingest_date"])] = m
    return dict(sorted(manifests.items()))


def run_checks(manifests: dict[date, dict[str, Any]], day: date) -> list[Check]:
    today = manifests.get(day)
    if today is None:
        return [Check("collected", "FAIL", f"no published snapshot for {day}", "missed-day")]
    earlier = [d for d in manifests if d < day]
    if not earlier:
        return [Check("collected", "OK", "first day: nothing to compare with")]
    prev_day = earlier[-1]
    prev = manifests[prev_day]

    return [
        _check_gap(day, prev_day),
        _check_refreshed(today, prev, prev_day),
        _check_row_drop(today, prev, prev_day),
        _check_slow(today, [manifests[d] for d in earlier[-SLOW_WINDOW:]]),
    ]


def _check_gap(day: date, prev_day: date) -> Check:
    gap = (day - prev_day).days - 1
    if gap == 0:
        return Check("no gap", "OK", f"previous day {prev_day} was collected")
    return Check(
        "no gap", "WARN",
        f"{gap} day(s) missing since {prev_day}: those days of history are lost for good",
        "missed-day",
    )


def _check_refreshed(today: dict, prev: dict, prev_day: date) -> Check:
    now, before = today.get("source_rows_updated_at"), prev.get("source_rows_updated_at")
    if now is None or before is None:
        return Check("source refreshed", "OK", "source version unknown (older manifest)")
    if now == before:
        return Check(
            "source refreshed", "WARN",
            f"the source did not reload since {prev_day} (rowsUpdatedAt {_when(now)}): "
            "today's snapshot repeats the last one, 0 changes expected",
            "source-not-refreshed",
        )
    return Check("source refreshed", "OK", f"source reloaded at {_when(now)}")


def _check_row_drop(today: dict, prev: dict, prev_day: date) -> Check:
    rows, before = today["rows"], prev["rows"]
    diff = rows - before
    detail = f"{rows:,} rows ({diff:+,} vs {prev_day})"
    if before and -diff / before > ROW_DROP_FAIL:
        return Check("row count", "FAIL", detail + f": dropped more than {ROW_DROP_FAIL:.0%}",
                     "row-count-drop")
    return Check("row count", "OK", detail)


def _check_slow(today: dict, previous: list[dict]) -> Check:
    seconds = today["seconds"]
    history = [m["seconds"] for m in previous if "seconds" in m]
    detail = f"download took {seconds / 60:.0f} min"
    if len(history) < SLOW_MIN_RUNS:
        return Check("run time", "OK", detail + " (not enough history to compare)")
    median = statistics.median(history)
    detail += f", median of the last {len(history)} runs {median / 60:.0f} min"
    if today.get("attempts", 1) > 1:
        detail += f", {today['attempts']} tries"
    if seconds > SLOW_FACTOR * median:
        return Check("run time", "WARN", detail, "slow-run")
    return Check("run time", "OK", detail)


def _when(unix_seconds: int) -> str:
    return datetime.fromtimestamp(unix_seconds, UTC).strftime("%Y-%m-%d %H:%M UTC")


def worst(checks: list[Check]) -> str:
    for status in ("FAIL", "WARN"):
        if any(c.status == status for c in checks):
            return status
    return "OK"


def format_message(checks: list[Check]) -> str:
    icon = {"OK": "OK  ", "WARN": "WARN", "FAIL": "FAIL"}
    return "\n".join(f"{icon[c.status]} {c.name}: {c.detail}" for c in checks)


def notify_result(day: date, checks: list[Check], **send_kwargs: Any) -> bool:
    status = worst(checks)
    first_problem = next((c for c in checks if c.status == status and c.runbook), None)
    priority, tags = {
        "OK": ("min", ("white_check_mark",)),  # silent: good news should not ring
        "WARN": ("high", ("warning",)),
        "FAIL": ("urgent", ("rotating_light",)),
    }[status]
    return notify.send(
        f"SECOP Ledger {day}: {status}",
        format_message(checks),
        priority=priority,
        tags=tags,
        runbook=first_problem.runbook if first_problem else None,
        **send_kwargs,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data checks after the daily run.")
    parser.add_argument("--date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--no-notify", action="store_true", help="only print the result")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    checks = run_checks(load_manifests(Settings.from_env().data_dir), args.date)
    for c in checks:
        log.log(logging.INFO if c.status == "OK" else logging.WARNING,
                "%s %s: %s", c.status, c.name, c.detail)
    if not args.no_notify:
        notify_result(args.date, checks)
    return EXIT_CHECK_FAILED if worst(checks) == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
