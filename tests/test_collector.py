"""Collector tests. No network: the API is replaced by a fake session."""

from __future__ import annotations

import json
from datetime import date

import pyarrow.parquet as pq
import pytest
import requests

from secop_ledger.config import Settings
from secop_ledger.ingest import raw_writer
from secop_ledger.ingest.collect import (
    CountMismatchError,
    SourceRefreshedError,
    check_count,
    collect,
)
from secop_ledger.ingest.columns import PII_COLUMNS, SELECTED_COLUMNS
from secop_ledger.ingest.keep_awake import ES_CONTINUOUS, ES_SYSTEM_REQUIRED, keep_awake
from secop_ledger.ingest.socrata import SocrataClient

COLUMNS = ("id_contrato", "valor_pagado", "urlproceso")
DAY = date(2026, 11, 10)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Returns the given pages in order and records the params of every call."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(params)
        return FakeResponse(self.pages.pop(0) if self.pages else [])


def make_rows(ids):
    return [
        {":id": f"row-{i}", "id_contrato": f"CO1.{i}", "valor_pagado": str(i * 10)} for i in ids
    ]


# --- Data minimization -------------------------------------------------------------------------

def test_no_personal_data_is_requested():
    assert not set(SELECTED_COLUMNS) & PII_COLUMNS


def test_selected_columns_have_no_duplicates():
    assert len(SELECTED_COLUMNS) == len(set(SELECTED_COLUMNS))


# --- Pagination --------------------------------------------------------------------------------

def test_keyset_pagination_continues_after_the_last_id_and_stops_on_a_short_page():
    session = FakeSession([make_rows([1, 2]), make_rows([3])])
    client = SocrataClient(session, "test-id")

    pages = list(client.iter_pages(COLUMNS, "x = 1", page_size=2))

    assert [len(p) for p in pages] == [2, 1]
    assert session.calls[0]["$where"] == "x = 1"
    assert session.calls[1]["$where"] == "(x = 1) AND :id > 'row-2'"
    assert all(call["$order"] == ":id" for call in session.calls)
    assert len(session.calls) == 2  # no extra request after the short page


def test_pagination_stops_on_an_empty_page():
    session = FakeSession([make_rows([1, 2]), []])
    pages = list(SocrataClient(session, "test-id").iter_pages(COLUMNS, "x = 1", page_size=2))
    assert [len(p) for p in pages] == [2]


# --- Retry when the connection breaks in the middle of a page -----------------------------------

class BrokenBodyResponse(FakeResponse):
    """Like the 2026-11-13 run: the response starts, then the connection is reset."""

    def json(self):
        raise requests.exceptions.ChunkedEncodingError("Connection broken: ConnectionResetError")


class FlakySession(FakeSession):
    """Breaks the body of the given call numbers (0 = first call), then answers normally."""

    def __init__(self, pages, broken_calls):
        super().__init__(pages)
        self.broken_calls = set(broken_calls)

    def get(self, url, params=None, timeout=None):
        call = len(self.calls)
        self.calls.append(params)
        if call in self.broken_calls:
            return BrokenBodyResponse(None)
        return FakeResponse(self.pages.pop(0) if self.pages else [])


def test_a_page_broken_in_the_middle_is_asked_again_with_the_same_keyset():
    session = FlakySession([make_rows([1, 2]), make_rows([3])], broken_calls={1})
    waits = []
    client = SocrataClient(session, "test-id", sleep=waits.append)

    pages = list(client.iter_pages(COLUMNS, "x = 1", page_size=2))

    assert [len(p) for p in pages] == [2, 1]  # no row lost, no row repeated
    assert session.calls[1] == session.calls[2]  # the retry asks for exactly the same page
    assert waits == [30]


def test_it_gives_up_after_the_last_retry():
    session = FlakySession([], broken_calls=range(10))
    waits = []
    client = SocrataClient(session, "test-id", sleep=waits.append)

    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        list(client.iter_pages(COLUMNS, "x = 1", page_size=2))

    assert len(session.calls) == 5  # 1 try + 4 retries
    assert waits == [30, 60, 90, 120]


# --- Raw writer --------------------------------------------------------------------------------

def test_values_are_stored_as_text_and_objects_as_json():
    rows = [{":id": "row-1", "id_contrato": "CO1.1", "valor_pagado": 5,
             "urlproceso": {"url": "https://example.org"}}]
    table = raw_writer.rows_to_table(rows, raw_writer.raw_schema(COLUMNS))
    record = table.to_pylist()[0]
    assert record["valor_pagado"] == "5"
    assert json.loads(record["urlproceso"]) == {"url": "https://example.org"}


def test_missing_keys_become_null():
    table = raw_writer.rows_to_table([{":id": "row-1"}], raw_writer.raw_schema(COLUMNS))
    assert table.to_pylist()[0]["valor_pagado"] is None


def _load_day(tmp_path, rows):
    written = raw_writer.write_staging([rows], COLUMNS, tmp_path, DAY)
    return raw_writer.publish(tmp_path, DAY, {"rows": written})


def test_loading_the_same_day_twice_replaces_it_and_never_appends(tmp_path):
    rows = make_rows([1, 2, 3])
    _load_day(tmp_path, rows)
    final_dir = _load_day(tmp_path, rows)

    files = list(final_dir.glob("*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).num_rows == 3


def test_a_corrected_reload_replaces_the_old_values(tmp_path):
    _load_day(tmp_path, make_rows([1, 2, 3]))
    corrected = make_rows([1, 2, 3])
    corrected[0]["valor_pagado"] = "999"
    final_dir = _load_day(tmp_path, corrected)

    table = pq.read_table(final_dir / "part-00000.parquet")
    assert table.num_rows == 3
    assert table.column("valor_pagado").to_pylist()[0] == "999"


def test_a_failed_run_publishes_nothing(tmp_path):
    def broken_pages():
        yield make_rows([1, 2])
        raise ConnectionError("network down")

    with pytest.raises(ConnectionError):
        raw_writer.write_staging(broken_pages(), COLUMNS, tmp_path, DAY)
    raw_writer.discard(tmp_path, DAY)

    assert not raw_writer.partition_dir(tmp_path, DAY).exists()
    assert not raw_writer.staging_dir(tmp_path, DAY).exists()


# --- Reconciliation ----------------------------------------------------------------------------

def test_count_within_tolerance_passes():
    check_count(expected=100_000, written=100_100)  # 0.1 %


def test_count_outside_tolerance_fails():
    with pytest.raises(CountMismatchError):
        check_count(expected=100_000, written=90_000)


def test_zero_rows_from_the_source_fails():
    with pytest.raises(CountMismatchError):
        check_count(expected=0, written=0)


# --- The source reloads in the middle of the download (2026-11-25) ------------------------------

def test_rows_updated_at_is_read_from_the_dataset_metadata():
    session = FakeSession([{"rowsUpdatedAt": 1795687978, "columns": []}])
    assert SocrataClient(session, "test-id").rows_updated_at() == 1795687978


class FakeClient:
    """A source that can reload between calls. `versions` = rowsUpdatedAt answers, in order."""

    def __init__(self, versions, rows_per_try):
        self.versions = list(versions)
        self.rows_per_try = list(rows_per_try)
        self.downloads = 0

    def column_names(self):
        return set(SELECTED_COLUMNS)

    def rows_updated_at(self):
        return self.versions.pop(0)

    def count(self, where):
        return len(self.rows_per_try[min(self.downloads, len(self.rows_per_try) - 1)])

    def iter_pages(self, columns, where, page_size):
        rows = self.rows_per_try[self.downloads]
        self.downloads += 1
        yield rows


def _settings(tmp_path):
    return Settings(data_dir=tmp_path, slice_start=date(2025, 1, 1), page_size=25_000,
                    app_token=None)


def test_a_reload_during_the_download_starts_again_and_publishes_the_second_read(tmp_path):
    # Try 1: version 100 at the start, 200 at the end (reloaded) -> thrown away.
    # Try 2: version 200 at the start and at the end -> published.
    old_ids, new_ids = make_rows([1, 2, 3]), make_rows([7, 8, 9, 10])
    client = FakeClient(versions=[100, 200, 200, 200], rows_per_try=[old_ids, new_ids])

    assert collect(_settings(tmp_path), DAY, client=client) == 0

    final_dir = raw_writer.partition_dir(tmp_path, DAY)
    table = pq.read_table(final_dir / "part-00000.parquet")
    assert table.column("socrata_id").to_pylist() == ["row-7", "row-8", "row-9", "row-10"]
    manifest = json.loads((final_dir / "_manifest.json").read_text(encoding="utf-8"))
    assert manifest["attempts"] == 2
    assert manifest["source_rows_updated_at"] == 200
    assert manifest["rows"] == manifest["source_count"] == 4


def test_reloads_on_every_try_publish_nothing(tmp_path):
    client = FakeClient(
        versions=[100, 200, 200, 300], rows_per_try=[make_rows([1]), make_rows([2])]
    )

    with pytest.raises(SourceRefreshedError):
        collect(_settings(tmp_path), DAY, client=client)

    assert client.downloads == 2
    assert not raw_writer.partition_dir(tmp_path, DAY).exists()
    assert not raw_writer.staging_dir(tmp_path, DAY).exists()


def test_a_count_mismatch_without_a_reload_still_fails_and_is_not_retried(tmp_path):
    class LyingCount(FakeClient):
        def count(self, where):
            return 1_000

    client = LyingCount(versions=[100, 100], rows_per_try=[make_rows([1, 2])])

    with pytest.raises(CountMismatchError):
        collect(_settings(tmp_path), DAY, client=client)
    assert client.downloads == 1


def test_keep_awake_blocks_sleep_and_always_gives_it_back():
    calls = []
    with pytest.raises(RuntimeError), keep_awake(set_state=lambda flags: calls.append(flags) or 1):
        raise RuntimeError("the download failed")

    assert calls == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED, ES_CONTINUOUS]
