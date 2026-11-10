"""Collector tests. No network: the API is replaced by a fake session."""

from __future__ import annotations

import json
from datetime import date

import pyarrow.parquet as pq
import pytest

from secop_ledger.ingest import raw_writer
from secop_ledger.ingest.collect import CountMismatchError, check_count
from secop_ledger.ingest.columns import PII_COLUMNS, SELECTED_COLUMNS
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
