"""Change detection tests, on tiny fake silver snapshots."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from secop_ledger.silver import rules
from secop_ledger.silver.build import row_hash_expr, silver_dir
from secop_ledger.silver.changes import build_changes

D1, D2, D3, D5 = date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 14)


def contract(n: int, pagado: int = 0, estado: str = "En ejecución", ultima: str | None = None):
    return {
        "id_contrato": f"C{n}",
        "estado_contrato": estado,
        "valor_del_contrato": 1000,
        "valor_facturado": 0,
        "valor_pagado": pagado,
        "valor_pendiente_de_pago": 1000 - pagado,
        "dias_adicionados": 0,
        "fecha_de_fin_del_contrato": "2026-12-31 00:00:00",
        "ultima_actualizacion": ultima,
    }


def write_snapshot(tmp_path, day, rows):
    """A silver snapshot with only the columns change detection reads, row_hash included."""
    folder = silver_dir(tmp_path, "contracts_snapshot", day)
    folder.mkdir(parents=True)
    con = duckdb.connect()
    con.sql("""
        CREATE TABLE s (id_contrato VARCHAR, estado_contrato VARCHAR,
            valor_del_contrato DECIMAL(38,2), valor_facturado DECIMAL(38,2),
            valor_pagado DECIMAL(38,2), valor_pendiente_de_pago DECIMAL(38,2),
            dias_adicionados BIGINT, fecha_de_fin_del_contrato TIMESTAMP,
            ultima_actualizacion TIMESTAMP)
    """)
    for r in rows:
        con.execute("INSERT INTO s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(r.values()))
    con.sql(
        f"COPY (SELECT *, {row_hash_expr(rules.TRACKED_COLUMNS)} AS row_hash FROM s) "
        f"TO '{(folder / 'part-00000.parquet').as_posix()}' (FORMAT parquet)"
    )


def read_changes(tmp_path, day):
    path = silver_dir(tmp_path, "contract_changes", day) / "part-00000.parquet"
    rel = duckdb.sql(f"SELECT * FROM read_parquet('{path.as_posix()}') ORDER BY id_contrato")
    return {r[0]: dict(zip(rel.columns, r, strict=True)) for r in rel.fetchall()}


def test_first_day_is_the_baseline(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1), contract(2)])
    m = build_changes(tmp_path, D1)
    assert m["baseline"] == 2
    assert {r["change_type"] for r in read_changes(tmp_path, D1).values()} == {"baseline"}


def test_new_changed_and_disappeared(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1), contract(2), contract(3)])
    write_snapshot(tmp_path, D2, [contract(1), contract(2, pagado=400), contract(4)])
    build_changes(tmp_path, D1)
    m = build_changes(tmp_path, D2)

    changes = read_changes(tmp_path, D2)
    assert {k: v["change_type"] for k, v in changes.items()} == {
        "C2": "changed", "C3": "disappeared", "C4": "new",
    }
    assert "C1" not in changes  # unchanged contracts are not stored
    assert (m["new"], m["changed"], m["disappeared"]) == (1, 1, 1)


def test_changed_columns_and_before_after(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1)])
    write_snapshot(tmp_path, D2, [contract(1, pagado=400, estado="Modificado")])
    build_changes(tmp_path, D1)
    m = build_changes(tmp_path, D2)

    c1 = read_changes(tmp_path, D2)["C1"]
    assert sorted(c1["changed_columns"]) == [
        "estado_contrato", "valor_pagado", "valor_pendiente_de_pago",
    ]
    assert c1["before"]["valor_pagado"] == 0
    assert c1["after"]["valor_pagado"] == 400
    assert m["changed_by_column"]["valor_pagado"] == 1


def test_dated_by_source_measures_the_watermark(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1), contract(2)])
    write_snapshot(tmp_path, D2, [
        contract(1, pagado=100, ultima="2026-11-11 08:00:00"),  # source updated its date
        contract(2, pagado=100),                               # changed, no date
    ])
    build_changes(tmp_path, D1)
    m = build_changes(tmp_path, D2)
    assert (m["changed_dated_by_source"], m["changed_not_dated_by_source"]) == (1, 1)


def test_a_gap_compares_with_the_last_snapshot_we_have(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1)])
    write_snapshot(tmp_path, D2, [contract(1)])
    write_snapshot(tmp_path, D5, [contract(1, pagado=50)])  # days 3 and 4 are missing
    m = build_changes(tmp_path, D5)
    assert m["prev_ingest_date"] == D2.isoformat()
    assert m["gap_days"] == 2
    assert read_changes(tmp_path, D5)["C1"]["prev_ingest_date"] == D2


def test_rebuilding_a_day_gives_the_same_result(tmp_path):
    write_snapshot(tmp_path, D1, [contract(1)])
    write_snapshot(tmp_path, D2, [contract(1, pagado=10), contract(2)])
    assert build_changes(tmp_path, D2) == build_changes(tmp_path, D2)


def test_today_equals_previous_plus_new_minus_disappeared(tmp_path):
    write_snapshot(tmp_path, D1, [contract(n) for n in range(10)])
    write_snapshot(tmp_path, D3, [contract(n) for n in range(3, 15)])
    m = build_changes(tmp_path, D3)
    assert m["snapshot_rows"] == m["prev_snapshot_rows"] + m["new"] - m["disappeared"]


def test_missing_snapshot_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        build_changes(tmp_path, D1)
