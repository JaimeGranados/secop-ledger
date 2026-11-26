"""Heartbeat tests. No network: requests.post is replaced by a fake."""

from __future__ import annotations

import pytest
import requests

from secop_ledger.monitor import heartbeat

BASE = "https://hc-ping.example/1234-uuid"


class FakeResponse:
    def __init__(self, status=200):
        self.status_code, self.ok, self.text = status, status < 400, ""


class FakePost:
    """`errors`: how many calls raise a network error before answering `status`."""

    def __init__(self, errors=0, status=200):
        self.errors, self.status, self.calls = errors, status, []

    def __call__(self, url, data=None, timeout=None):
        self.calls.append({"url": url, "data": data})
        if len(self.calls) <= self.errors:
            raise requests.exceptions.ConnectionError("network blip")
        return FakeResponse(self.status)


def test_start_and_exit_codes_go_to_the_right_urls():
    assert heartbeat.ping_url(BASE + "/", "start") == BASE + "/start"
    assert heartbeat.ping_url(BASE, "exit", 0) == BASE + "/0"
    assert heartbeat.ping_url(BASE, "exit", 2) == BASE + "/2"  # healthchecks: non-zero = failed


def test_the_log_tail_goes_in_the_body():
    post = FakePost()
    assert heartbeat.ping("exit", 3, body="x" * 20_000 + "Traceback", base_url=BASE, post=post)
    data = post.calls[0]["data"]
    assert len(data) == heartbeat.MAX_BODY_CHARS and data.endswith(b"Traceback")


def test_a_network_blip_is_retried():
    post = FakePost(errors=2)
    assert heartbeat.ping("start", base_url=BASE, post=post) is True
    assert len(post.calls) == 3


def test_it_never_raises_when_healthchecks_is_down():
    post = FakePost(errors=99)
    assert heartbeat.ping("start", base_url=BASE, post=post) is False
    assert len(post.calls) == heartbeat.TRIES


def test_a_refused_ping_is_not_retried():
    post = FakePost(status=404)
    assert heartbeat.ping("exit", 0, base_url=BASE, post=post) is False
    assert len(post.calls) == 1


def test_no_url_means_no_request(monkeypatch):
    monkeypatch.setattr(heartbeat, "load_dotenv", lambda: None)
    monkeypatch.delenv("HC_PING_URL", raising=False)
    post = FakePost()
    assert heartbeat.ping("start", post=post) is False
    assert post.calls == []


def test_exit_needs_a_code():
    with pytest.raises(SystemExit):
        heartbeat.main(["exit"])
