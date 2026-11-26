"""Heartbeat to healthchecks.io: a dead man's switch for the daily run.

Every other alert needs our code to run. If the PC is off, Python is broken or the run hangs,
nothing in this repo can say so. So the pipeline tells an OUTSIDE service "I'm alive", and the
service alerts when the signal does NOT arrive. Silence is the alarm (it fails safe).

    start      ping <HC_PING_URL>/start     the run began (healthchecks also measures the run time)
    exit N     ping <HC_PING_URL>/N         0 = success, other = failed (log tail in the body)
    nothing    by the deadline              healthchecks.io alerts: PC off, or the run hung

Like the notifier, a ping NEVER fails the pipeline: if healthchecks.io is down, we log it.
The ping URL has a secret UUID (anyone with it can mark the check up/down): it lives only in .env.

Run (from the daily script):
    uv run python -m secop_ledger.monitor.heartbeat start
    uv run python -m secop_ledger.monitor.heartbeat exit 0 --log data/logs/collect-2026-11-27.log
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from secop_ledger.monitor.notify import tail

log = logging.getLogger("secop_ledger.heartbeat")

TIMEOUT = (5, 10)
TRIES = 3  # a ping is tiny; try again on a network blip
MAX_BODY_CHARS = 10_000  # healthchecks keeps up to 100 kB per ping; the log tail is enough


def ping_url(base: str, kind: str, exit_code: int | None = None) -> str:
    base = base.rstrip("/")
    if kind == "start":
        return f"{base}/start"
    if kind == "exit":
        return f"{base}/{exit_code}"
    raise ValueError(f"unknown ping kind: {kind}")


def ping(
    kind: str,
    exit_code: int | None = None,
    body: str = "",
    base_url: str | None = None,
    post: Callable[..., Any] = requests.post,  # tests pass a fake
) -> bool:
    """Send one ping. Returns True if healthchecks.io accepted it. Never raises."""
    if base_url is None:
        load_dotenv()
        base_url = os.getenv("HC_PING_URL") or None
    if not base_url:
        log.warning("HC_PING_URL is not set; heartbeat not sent (%s)", kind)
        return False
    url = ping_url(base_url, kind, exit_code)
    data = body[-MAX_BODY_CHARS:].encode("utf-8")
    for attempt in range(1, TRIES + 1):
        try:
            response = post(url, data=data, timeout=TIMEOUT)
            if response.ok:
                log.info("heartbeat sent: %s%s", kind, "" if exit_code is None else f" {exit_code}")
                return True
            log.error("healthchecks refused the ping (HTTP %s): %s",
                      response.status_code, response.text[:200])
            return False  # a 4xx won't change on retry (wrong URL)
        except Exception as error:
            log.warning("heartbeat try %d of %d failed: %s", attempt, TRIES, error)
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heartbeat ping to healthchecks.io.")
    parser.add_argument("kind", choices=("start", "exit"))
    parser.add_argument("code", nargs="?", type=int, help="exit code of the run (for 'exit')")
    parser.add_argument("--log", type=Path, help="log file; its last lines go with the ping")
    args = parser.parse_args(argv)
    if args.kind == "exit" and args.code is None:
        parser.error("'exit' needs the exit code, e.g. 'exit 0'")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    body = tail(args.log, 30) if args.log else ""
    ping(args.kind, args.code, body)
    return 0  # never change the run's result


if __name__ == "__main__":
    sys.exit(main())
