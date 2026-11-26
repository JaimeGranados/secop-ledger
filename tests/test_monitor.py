"""Monitoring tests: data checks on fake manifests, and the ntfy request. No network."""

from __future__ import annotations

import json
from datetime import date, timedelta

import requests

from secop_ledger.monitor import checks, notify
from secop_ledger.monitor.checks import load_manifests, run_checks, worst

SETTINGS = notify.NotifySettings(topic="secop-test", server="https://ntfy.example", email=None)
D = date(2026, 11, 26)


def manifest(day, rows=2_000_000, seconds=3000, version=None, attempts=1):
    m = {"ingest_date": day.isoformat(), "rows": rows, "seconds": seconds, "attempts": attempts}
    if version is not None:
        m["source_rows_updated_at"] = version
    return m


def history(n_days=5, **today):
    """n_days of normal runs before D (one per day, a new source version each day), then D."""
    days = {D - timedelta(days=i): manifest(D - timedelta(days=i), version=1000 + i)
            for i in range(n_days, 0, -1)}
    days[D] = manifest(D, **{"version": 999, **today})
    return days


def by_name(result):
    return {c.name: c for c in result}


# --- Checks ------------------------------------------------------------------------------------

def test_a_normal_day_is_all_ok():
    result = run_checks(history(rows=2_000_500), D)
    assert worst(result) == "OK"


def test_a_missing_day_before_today_is_a_warning():
    days = history()
    del days[D - timedelta(days=1)]
    c = by_name(run_checks(days, D))["no gap"]
    assert c.status == "WARN" and c.runbook == "missed-day"
    assert "1 day(s) missing" in c.detail


def test_the_same_source_version_as_yesterday_is_a_warning():
    days = history()
    days[D]["source_rows_updated_at"] = days[D - timedelta(days=1)]["source_rows_updated_at"]
    c = by_name(run_checks(days, D))["source refreshed"]
    assert c.status == "WARN" and c.runbook == "source-not-refreshed"


def test_old_manifests_without_a_version_are_not_an_alert():
    days = {D - timedelta(days=1): manifest(D - timedelta(days=1)), D: manifest(D)}
    assert by_name(run_checks(days, D))["source refreshed"].status == "OK"


def test_losing_more_than_one_percent_of_rows_fails():
    c = by_name(run_checks(history(rows=1_970_000), D))["row count"]
    assert c.status == "FAIL" and c.runbook == "row-count-drop"


def test_a_small_drop_is_ok():
    assert by_name(run_checks(history(rows=1_999_000), D))["row count"].status == "OK"


def test_a_run_slower_than_twice_the_median_is_a_warning():
    c = by_name(run_checks(history(seconds=8900), D))["run time"]
    assert c.status == "WARN" and c.runbook == "slow-run"


def test_no_slow_alert_without_enough_history():
    assert by_name(run_checks(history(n_days=2, seconds=8900), D))["run time"].status == "OK"


def test_no_snapshot_for_the_day_fails():
    result = run_checks(history(), D + timedelta(days=1))
    assert worst(result) == "FAIL" and result[0].runbook == "missed-day"


def test_manifests_in_tmp_folders_are_ignored(tmp_path):
    base = tmp_path / "raw" / "contracts"
    for folder in ("ingest_date=2026-11-25.tmp", "ingest_date=2026-11-26"):
        (base / folder).mkdir(parents=True)
        (base / folder / "_manifest.json").write_text(
            json.dumps(manifest(D)), encoding="utf-8")
    assert list(load_manifests(tmp_path)) == [D]


def test_main_exits_5_on_a_failed_check(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOP_DATA_DIR", str(tmp_path))
    assert checks.main(["--date", "2026-11-26", "--no-notify"]) == checks.EXIT_CHECK_FAILED


# --- Notifications -----------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, text="{}"):
        self.status_code, self.text, self.ok = status, text, status < 400


class FakePost:
    """`refuse_email`: answer 400 to any request with an Email header (like ntfy.sh, anonymous)."""

    def __init__(self, fail=False, refuse_email=False):
        self.fail = fail
        self.refuse_email = refuse_email
        self.calls = []

    def __call__(self, url, data=None, headers=None, timeout=None):
        self.calls.append({"url": url, "data": data, "headers": headers})
        if self.fail:
            raise requests.exceptions.ConnectionError("ntfy is down")
        if self.refuse_email and "Email" in headers:
            return FakeResponse(400, '{"code":40053,"error":"anonymous email not allowed"}')
        return FakeResponse()


def test_the_request_goes_to_the_topic_with_title_priority_and_runbook():
    url, headers, body = notify.build_request(
        SETTINGS, "title", "body", priority="urgent", tags=("warning",), runbook="slow-run")
    assert url == "https://ntfy.example/secop-test"
    assert headers["Priority"] == "urgent" and headers["Tags"] == "warning"
    assert headers["Click"].endswith("runbooks.md#slow-run")
    assert "Email" not in headers
    assert body == b"body"


def test_the_token_goes_in_the_authorization_header():
    with_token = notify.NotifySettings("secop-test", "https://ntfy.example", None, "tk_abc")
    _, headers, _ = notify.build_request(with_token, "t", "m")
    assert headers["Authorization"] == "Bearer tk_abc"


def test_a_refused_email_still_sends_the_push(caplog):
    post = FakePost(refuse_email=True)
    with_email = notify.NotifySettings("secop-test", "https://ntfy.example", "me@example.org")

    assert notify.send("t", "m", settings=with_email, post=post) is True

    assert ["Email" in c["headers"] for c in post.calls] == [True, False]
    assert "40053" in caplog.text  # the reason from ntfy is in the log, not only "400"


def test_the_email_header_is_added_only_when_set():
    with_email = notify.NotifySettings("secop-test", "https://ntfy.example", "me@example.org")
    _, headers, _ = notify.build_request(with_email, "t", "m")
    assert headers["Email"] == "me@example.org"


def test_a_long_message_keeps_its_end():
    _, _, body = notify.build_request(SETTINGS, "t", "x" * 5000 + "THE ERROR")
    assert len(body) < 3100 and body.endswith(b"THE ERROR")


def test_a_failed_notification_never_raises():
    post = FakePost(fail=True)
    assert notify.send("t", "m", settings=SETTINGS, post=post) is False
    assert len(post.calls) == 1


def test_no_topic_means_no_request():
    post = FakePost()
    off = notify.NotifySettings(topic=None, server="https://ntfy.example", email=None)
    assert notify.send("t", "m", settings=off, post=post) is False
    assert post.calls == []


def test_an_ok_day_is_sent_silently_and_a_failed_day_urgently():
    post = FakePost()
    checks.notify_result(D, run_checks(history(), D), settings=SETTINGS, post=post)
    checks.notify_result(D, run_checks(history(rows=1), D), settings=SETTINGS, post=post)
    assert [c["headers"]["Priority"] for c in post.calls] == ["min", "urgent"]
    assert post.calls[1]["headers"]["Click"].endswith("#row-count-drop")
