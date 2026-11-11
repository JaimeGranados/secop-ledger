"""Silver tests: each one checks one rule from the profile (docs/data-model.md, section 4).

They build a tiny raw day with the real raw writer, run silver, and read the result.
"""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pytest

from secop_ledger.ingest import raw_writer
from secop_ledger.ingest.columns import SELECTED_COLUMNS
from secop_ledger.silver.build import UnknownStatusError, build_day, silver_dir

DAY = date(2026, 11, 10)


def contract(n: int, **overrides) -> dict:
    """A valid raw row (all text, like the API sends it)."""
    row = {
        ":id": f"row-{n:04d}",
        "id_contrato": f"CO1.PCCNTR.{n}",
        "estado_contrato": "En ejecución",
        "fecha_de_firma": "2025-03-03T19:13:06.908",
        "fecha_de_inicio_del_contrato": "2025-03-04T00:00:00.000",
        "fecha_de_fin_del_contrato": "2025-12-31T00:00:00.000",
        "valor_del_contrato": "1000000",
        "valor_facturado": "500000",
        "valor_pagado": "400000",
        "valor_pendiente_de_pago": "600000",
        "dias_adicionados": "0",
        "ciudad": "Valledupar",
        "objeto_del_contrato": "Prestación de servicios",
    }
    row.update(overrides)
    return row


def build(tmp_path, rows):
    written = raw_writer.write_staging([rows], SELECTED_COLUMNS, tmp_path, DAY)
    raw_writer.publish(tmp_path, DAY, {"rows": written})
    return build_day(tmp_path, DAY)


def read(tmp_path, table):
    path = silver_dir(tmp_path, f"contracts_{table}", DAY) / "part-00000.parquet"
    rel = duckdb.sql(f"SELECT * FROM read_parquet('{path.as_posix()}') ORDER BY socrata_id")
    return [dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()]


# --- Reconciliation ------------------------------------------------------------------------------

def test_every_raw_row_is_explained(tmp_path):
    rows = [
        contract(1),
        contract(2), {**contract(2), ":id": "row-0099"},   # identical copy
        contract(3, id_contrato=None),                    # unusable
    ]
    m = build(tmp_path, rows)
    assert m["raw_rows"] == 4
    assert m["snapshot_rows"] + m["quarantine_rows"] + m["duplicate_copies_removed"] == 4


def test_building_the_same_day_twice_gives_the_same_result(tmp_path):
    rows = [contract(1), contract(2)]
    first = build(tmp_path, rows)
    second = build_day(tmp_path, DAY)
    assert first == second
    assert len(read(tmp_path, "snapshot")) == 2


# --- Uniqueness ----------------------------------------------------------------------------------

def test_identical_copies_keep_one_row(tmp_path):
    copy = {**contract(1), ":id": "row-0500"}
    m = build(tmp_path, [contract(1), copy])
    snapshot = read(tmp_path, "snapshot")
    assert [r["socrata_id"] for r in snapshot] == ["row-0001"]  # always the lowest id
    assert m["duplicate_copies_removed"] == 1


def test_copies_that_differ_go_to_quarantine_both(tmp_path):
    other = contract(1, valor_pagado="999999")
    other[":id"] = "row-0500"
    m = build(tmp_path, [contract(1), other, contract(2)])
    assert [r["id_contrato"] for r in read(tmp_path, "snapshot")] == ["CO1.PCCNTR.2"]
    assert m["conflicting_duplicate_ids"] == 1
    assert m["quarantine_reasons"] == {"conflicting_duplicates": 2}


# --- Validity ------------------------------------------------------------------------------------

def test_values_get_real_types(tmp_path):
    build(tmp_path, [contract(1)])
    row = read(tmp_path, "snapshot")[0]
    assert str(row["valor_del_contrato"]) == "1000000.00"
    assert row["dias_adicionados"] == 0
    assert row["fecha_de_firma"].year == 2025


def test_missing_required_column_goes_to_quarantine(tmp_path):
    m = build(tmp_path, [contract(1, fecha_de_firma=None), contract(2)])
    assert m["quarantine_reasons"] == {"missing_fecha_de_firma": 1}


def test_value_that_does_not_convert_goes_to_quarantine_with_its_raw_text(tmp_path):
    build(tmp_path, [contract(1, valor_pagado="12,5 millones")])
    quarantined = read(tmp_path, "quarantine")[0]
    assert quarantined["quarantine_reason"] == "invalid_valor_pagado"
    assert quarantined["valor_pagado"] == "12,5 millones"  # raw text kept for inspection


# --- Flags: suspicious rows stay, marked ---------------------------------------------------------

def test_impossible_values_are_flagged_not_deleted(tmp_path):
    rows = [
        contract(1, valor_del_contrato="944187311000000"),
        contract(2, fecha_de_fin_del_contrato="5025-12-20T00:00:00.000"),
        contract(3, valor_pagado="2000000"),
        contract(4),
    ]
    m = build(tmp_path, rows)
    snapshot = {r["id_contrato"]: r for r in read(tmp_path, "snapshot")}
    assert len(snapshot) == 4
    assert snapshot["CO1.PCCNTR.1"]["flag_implausible_value"]
    assert snapshot["CO1.PCCNTR.2"]["flag_implausible_date"]
    assert snapshot["CO1.PCCNTR.3"]["flag_paid_over_value"]
    assert not any(v for k, v in snapshot["CO1.PCCNTR.4"].items() if k.startswith("flag_"))
    assert m["flags"]["flag_implausible_value"] == 1


# --- Consistency ---------------------------------------------------------------------------------

def test_status_is_cleaned_and_grouped(tmp_path):
    build(tmp_path, [contract(1, estado_contrato="terminado")])
    row = read(tmp_path, "snapshot")[0]
    assert row["estado_contrato"] == "Terminado"
    assert row["estado_grupo"] == "closed"
    assert row["estado_contrato_raw"] == "terminado"


def test_unknown_status_stops_the_build_and_publishes_nothing(tmp_path):
    with pytest.raises(UnknownStatusError, match="Liquidado"):
        build(tmp_path, [contract(1, estado_contrato="Liquidado")])
    assert not silver_dir(tmp_path, "contracts_snapshot", DAY).exists()


def test_placeholder_becomes_null(tmp_path):
    build(tmp_path, [contract(1, ciudad="No Definido")])
    assert read(tmp_path, "snapshot")[0]["ciudad"] is None


# --- row_hash ------------------------------------------------------------------------------------

def _hash_of(tmp_path, **overrides):
    build(tmp_path, [contract(1, **overrides)])
    return read(tmp_path, "snapshot")[0]["row_hash"]


def test_row_hash_changes_when_a_tracked_column_changes(tmp_path):
    assert _hash_of(tmp_path) != _hash_of(tmp_path, valor_pagado="450000")


def test_row_hash_ignores_untracked_columns_and_status_case(tmp_path):
    base = _hash_of(tmp_path)
    assert base == _hash_of(tmp_path, objeto_del_contrato="Texto corregido")
    assert _hash_of(tmp_path, estado_contrato="terminado") == _hash_of(
        tmp_path, estado_contrato="Terminado"
    )


def test_manifest_is_written(tmp_path):
    m = build(tmp_path, [contract(1)])
    path = silver_dir(tmp_path, "contracts_snapshot", DAY) / "_manifest.json"
    assert json.loads(path.read_text(encoding="utf-8")) == m
