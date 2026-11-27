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
from secop_ledger.silver.build import (
    SNAPSHOT_COLUMNS,
    OldLayoutError,
    UnknownStatusError,
    build_day,
    silver_dir,
)

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


def build(tmp_path, rows, day=DAY):
    written = raw_writer.write_staging([rows], SELECTED_COLUMNS, tmp_path, day)
    raw_writer.publish(tmp_path, day, {"rows": written})
    return build_day(tmp_path, day)


def read(tmp_path, table, day=DAY):
    """table: snapshot (narrow, every contract), attributes (full rows, delta) or quarantine."""
    path = silver_dir(tmp_path, f"contracts_{table}", day) / "part-00000.parquet"
    order = "socrata_id" if table == "quarantine" else "id_contrato"
    rel = duckdb.sql(f"SELECT * FROM read_parquet('{path.as_posix()}') ORDER BY {order}")
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
    assert len(read(tmp_path, "snapshot")) == 1
    kept = read(tmp_path, "attributes")
    assert [r["socrata_id"] for r in kept] == ["row-0001"]  # always the lowest id
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
    row = read(tmp_path, "attributes")[0]
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
    row = read(tmp_path, "attributes")[0]
    assert row["estado_contrato"] == "Terminado"
    assert row["estado_grupo"] == "closed"
    assert row["estado_contrato_raw"] == "terminado"
    assert read(tmp_path, "snapshot")[0]["estado_contrato"] == "Terminado"


def test_unknown_status_stops_the_build_and_publishes_nothing(tmp_path):
    with pytest.raises(UnknownStatusError, match="Liquidado"):
        build(tmp_path, [contract(1, estado_contrato="Liquidado")])
    assert not silver_dir(tmp_path, "contracts_snapshot", DAY).exists()


def test_placeholder_becomes_null(tmp_path):
    build(tmp_path, [contract(1, ciudad="No Definido")])
    assert read(tmp_path, "attributes")[0]["ciudad"] is None


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


# --- Narrow snapshot + attributes delta ----------------------------------------------------------

DAY_2, DAY_4 = date(2026, 11, 11), date(2026, 11, 13)


def test_the_snapshot_is_narrow_and_the_first_day_has_every_full_row(tmp_path):
    m = build(tmp_path, [contract(1), contract(2)])
    snapshot = read(tmp_path, "snapshot")
    # (DuckDB adds ingest_date from the folder name: hive partitioning)
    assert tuple(c for c in snapshot[0] if c != "ingest_date") == SNAPSHOT_COLUMNS
    assert "objeto_del_contrato" not in snapshot[0]
    assert len(read(tmp_path, "attributes")) == 2  # the starting photo of the full rows
    assert (m["attribute_rows"], m["prev_snapshot_date"]) == (2, None)


def test_attributes_keep_only_new_and_changed_contracts(tmp_path):
    build(tmp_path, [contract(1), contract(2), contract(3), contract(4)])
    m = build(tmp_path, [
        contract(1),                                         # same as yesterday
        contract(2, valor_pagado="450000"),                  # tracked column changed
        contract(3, objeto_del_contrato="Texto corregido"),  # only an untracked text changed
        {**contract(4), ":id": "row-9004"},                  # only the source row id (a reload)
        contract(5),                                         # new
    ], day=DAY_2)

    assert len(read(tmp_path, "snapshot", DAY_2)) == 5  # the snapshot always has every contract
    changed = {r["id_contrato"]: r for r in read(tmp_path, "attributes", DAY_2)}
    assert sorted(changed) == ["CO1.PCCNTR.2", "CO1.PCCNTR.3", "CO1.PCCNTR.5"]
    assert changed["CO1.PCCNTR.3"]["objeto_del_contrato"] == "Texto corregido"
    assert (m["attribute_rows"], m["prev_snapshot_date"]) == (3, DAY.isoformat())


def test_after_a_gap_attributes_compare_with_the_last_snapshot_we_have(tmp_path):
    build(tmp_path, [contract(1), contract(2)])
    m = build(tmp_path, [contract(1), contract(2, ciudad="Bogotá")], day=DAY_4)
    assert m["prev_snapshot_date"] == DAY.isoformat()
    assert [r["id_contrato"] for r in read(tmp_path, "attributes", DAY_4)] == ["CO1.PCCNTR.2"]


def test_newest_attribute_row_rebuilds_the_current_full_row(tmp_path):
    """The rule the dbt test checks on real data: for every contract in the newest snapshot,
    its newest attribute row has the same full_hash."""
    build(tmp_path, [contract(1), contract(2)])
    build(tmp_path, [contract(1, ciudad="Bogotá"), contract(2), contract(3)], day=DAY_2)
    build(tmp_path, [contract(1, ciudad="Bogotá"), contract(3, valor_pagado="1")], day=DAY_4)

    root = (tmp_path / "silver").as_posix()
    mismatches = duckdb.sql(f"""
        WITH latest AS (
            SELECT id_contrato, full_hash
            FROM read_parquet('{root}/contracts_attributes/*/*.parquet', hive_partitioning = true)
            QUALIFY ingest_date = max(ingest_date) OVER (PARTITION BY id_contrato)
        )
        SELECT s.id_contrato
        FROM read_parquet('{root}/contracts_snapshot/ingest_date={DAY_4}/*.parquet') s
        LEFT JOIN latest l USING (id_contrato)
        WHERE l.full_hash IS DISTINCT FROM s.full_hash
    """).fetchall()
    assert mismatches == []


def test_a_previous_snapshot_with_the_old_layout_stops_the_build(tmp_path):
    old = silver_dir(tmp_path, "contracts_snapshot", DAY)
    old.mkdir(parents=True)
    duckdb.sql(
        f"COPY (SELECT 'CO1.PCCNTR.1' AS id_contrato, 'x' AS row_hash) "
        f"TO '{(old / 'part-00000.parquet').as_posix()}' (FORMAT parquet)"
    )
    with pytest.raises(OldLayoutError, match="--all"):
        build(tmp_path, [contract(1)], day=DAY_2)
    assert not silver_dir(tmp_path, "contracts_snapshot", DAY_2).exists()


def test_manifest_is_written(tmp_path):
    m = build(tmp_path, [contract(1)])
    path = silver_dir(tmp_path, "contracts_snapshot", DAY) / "_manifest.json"
    assert json.loads(path.read_text(encoding="utf-8")) == m


# --- DuckDB limits -----------------------------------------------------------------------------

def test_duckdb_uses_the_limits_for_a_small_pc(tmp_path, monkeypatch):
    from secop_ledger.silver import db

    monkeypatch.delenv("SECOP_DUCKDB_THREADS", raising=False)
    con = db.connect(tmp_path)
    assert con.sql("SELECT current_setting('threads')").fetchall()[0][0] == db.DEFAULT_THREADS

    monkeypatch.setenv("SECOP_DUCKDB_THREADS", "1")
    monkeypatch.setenv("SECOP_DUCKDB_MEMORY_LIMIT", "500MB")
    con = db.connect(tmp_path)
    assert con.sql("SELECT current_setting('threads')").fetchall()[0][0] == 1
    assert "MiB" in con.sql("SELECT current_setting('memory_limit')").fetchall()[0][0]
