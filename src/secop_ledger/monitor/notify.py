"""Send a notification with ntfy (https://ntfy.sh): a phone push, plus an email if ALERT_EMAIL
is set.

One HTTP POST to <NTFY_SERVER>/<NTFY_TOPIC>. The topic name works as a password: anyone who knows
it can read the messages, so it is random, lives only in .env, and messages carry no personal data.

Email: ntfy.sh does not send emails for anonymous users (error 40053). It needs a free account
with a verified email and an access token (NTFY_TOKEN, `Authorization: Bearer tk_...`). Free
limit: 5 emails per day. The token gives full access to the ntfy account, so it lives only in .env.

Three rules:
- A notification NEVER fails the pipeline. If ntfy is down, we log it and go on: losing an alert
  is bad, losing a day of data because of an alert is worse.
- No sound for good news. A daily "all OK" goes with priority `min` (silent); only problems ring.
  If the phone rings every day, you stop looking at it (alert fatigue).
- Degrade, don't drop. If ntfy refuses the email (daily limit used, email not verified), the
  same message is sent again without it: the phone push is the part that must arrive.

Run from the daily script when a step fails:
    uv run python -m secop_ledger.monitor.notify --step collect --exit-code 2 --log data/logs/x.log
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

log = logging.getLogger("secop_ledger.notify")

DEFAULT_SERVER = "https://ntfy.sh"
RUNBOOKS_URL = "https://github.com/JaimeGranados/secop-ledger/blob/develop/docs/runbooks.md"
MAX_MESSAGE_CHARS = 3000  # ntfy turns longer messages into attachments
TIMEOUT = (5, 20)

# Collector exit codes (see ingest/collect.py) -> runbook section.
COLLECT_EXIT_RUNBOOK = {
    1: "schema-drift",
    2: "count-mismatch",
    3: "step-failed",
    4: "source-reloaded-during-the-download",
}


@dataclass(frozen=True)
class NotifySettings:
    topic: str | None
    server: str
    email: str | None
    token: str | None = None

    @classmethod
    def from_env(cls) -> NotifySettings:
        load_dotenv()
        return cls(
            topic=os.getenv("NTFY_TOPIC") or None,
            server=(os.getenv("NTFY_SERVER") or DEFAULT_SERVER).rstrip("/"),
            email=os.getenv("ALERT_EMAIL") or None,  # an address, or "yes" = the account's email
            token=os.getenv("NTFY_TOKEN") or None,
        )


def build_request(
    settings: NotifySettings,
    title: str,
    message: str,
    priority: str = "high",
    tags: tuple[str, ...] = (),
    runbook: str | None = None,
    with_email: bool = True,
) -> tuple[str, dict[str, str], bytes]:
    """URL, headers and body of the ntfy POST (kept apart from sending, so tests can check it)."""
    if len(message) > MAX_MESSAGE_CHARS:
        message = "...\n" + message[-MAX_MESSAGE_CHARS:]  # the end of a log matters most
    headers = {"Title": title, "Priority": priority}
    if tags:
        headers["Tags"] = ",".join(tags)
    if runbook:
        headers["Click"] = f"{RUNBOOKS_URL}#{runbook}"  # tapping the push opens the runbook
    if settings.token:
        headers["Authorization"] = f"Bearer {settings.token}"
    if settings.email and with_email:
        headers["Email"] = settings.email
    return f"{settings.server}/{settings.topic}", headers, message.encode("utf-8")


def send(
    title: str,
    message: str,
    priority: str = "high",
    tags: tuple[str, ...] = (),
    runbook: str | None = None,
    settings: NotifySettings | None = None,
    post: Callable[..., Any] = requests.post,  # tests pass a fake
) -> bool:
    """Send one notification. Returns True if ntfy accepted it. Never raises."""
    settings = settings or NotifySettings.from_env()
    if not settings.topic:
        log.warning("NTFY_TOPIC is not set; notification not sent: %s", title)
        return False
    tries = [True, False] if settings.email else [False]  # with email first, then push only
    for with_email in tries:
        url, headers, body = build_request(
            settings, title, message, priority, tags, runbook, with_email
        )
        try:
            response = post(url, data=body, headers=headers, timeout=TIMEOUT)
        except Exception:
            log.exception("could not reach ntfy: %s", title)
            return False  # the server is down: sending without email would fail too
        if response.ok:
            log.info("notification sent%s: %s", "" if with_email else " (push only)", title)
            return True
        # ntfy explains the error in the body ({"code": 40053, "error": "..."}): log it.
        log.error("ntfy refused the notification (HTTP %s%s): %s",
                  response.status_code, ", with email" if with_email else "", response.text[:300])
    return False


def tail(path: Path, lines: int = 15) -> str:
    try:
        return "".join(path.read_text(encoding="utf-8", errors="replace").splitlines(True)[-lines:])
    except OSError:
        return f"(could not read {path})"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Alert that a pipeline step failed.")
    parser.add_argument("--step", required=True, help="collect, silver, changes, dbt or checks")
    parser.add_argument("--exit-code", type=int, required=True)
    parser.add_argument("--log", type=Path, help="log file; its last lines go in the message")
    parser.add_argument("--test", action="store_true", help="send a test notification and stop")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    if args.test:
        ok = send("SECOP Ledger: test", "If you can read this, alerts work.", priority="default",
                  tags=("white_check_mark",))
        return 0 if ok else 1

    runbook = "step-failed"
    if args.step == "collect":
        runbook = COLLECT_EXIT_RUNBOOK.get(args.exit_code, "step-failed")
    message = f"Step '{args.step}' failed with exit code {args.exit_code}.\n\n"
    if args.log:
        message += f"Last lines of {args.log}:\n{tail(args.log)}"
    send(f"SECOP Ledger: {args.step} FAILED", message, priority="urgent",
         tags=("rotating_light",), runbook=runbook)
    return 0  # the script exits with the step's own code, not ours


if __name__ == "__main__":
    sys.exit(main())
