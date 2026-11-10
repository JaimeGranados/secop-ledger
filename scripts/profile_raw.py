"""Profile one raw snapshot with DuckDB, to design the silver rules from real data.

Run:  uv run python scripts/profile_raw.py [--date YYYY-MM-DD]   (default: latest snapshot)

Each section answers one data quality dimension: uniqueness, validity, consistency,
completeness and distribution. It only reads data; it writes nothing.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from secop_ledger.config import Settings

TRACKED = (
    "estado_contrato",
    "valor_del_contrato",
    "valor_facturado",
    "valor_pagado",
    "valor_pendiente_de_pago",
    "dias_adicionados",
    "fecha_de_fin_del_contrato",
)
MONEY = (
    "valor_del_contrato",
    "valor_facturado",
    "valor_pagado",
    "valor_pendiente_de_pago",
    "valor_de_pago_adelantado",
)
DATES = (
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "ultima_actualizacion",
)
CATEGORIES = (
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "orden",
    "sector",
    "tipodocproveedor",
)


def latest_date(data_dir: Path) -> str:
    folders = (data_dir / "raw" / "contracts").glob("ingest_date=*")
    days = sorted(p.name.split("=")[1] for p in folders if not p.name.endswith(".tmp"))
    if not days:
        raise SystemExit("no raw snapshots found")
    return days[-1]


def section(title: str) -> None:
    print(f"\n{'=' * 100}\n{title}\n{'=' * 100}")


def show(con: duckdb.DuckDBPyConnection, sql: str, rows: int = 60) -> None:
    """Print a query result as a plain-text table (the Windows console can't draw box lines)."""
    rel = con.sql(sql)
    header = rel.columns
    data = [["NULL" if v is None else str(v)[:60] for v in r] for r in rel.limit(rows).fetchall()]
    widths = [max(len(h), *(len(r[i]) for r in data)) if data else len(h)
              for i, h in enumerate(header)]
    print("  ".join(h.ljust(w) for h, w in zip(header, widths, strict=True)))
    print("  ".join("-" * w for w in widths))
    for r in data:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)))
    if not data:
        print("(no rows)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="ingestion date (default: latest)")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # accents in the data

    settings = Settings.from_env()
    day = args.date or latest_date(settings.data_dir)
    path = settings.data_dir / "raw" / "contracts" / f"ingest_date={day}" / "*.parquet"

    con = duckdb.connect()
    con.sql(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{path.as_posix()}')")
    print(f"Profiling raw snapshot {day}")

    # ------------------------------------------------------------------------------------------
    section("1. UNIQUENESS - rows, ids, duplicates")
    show(con, """
        SELECT count(*)                    AS rows,
               count(DISTINCT socrata_id)  AS distinct_socrata_id,
               count(DISTINCT id_contrato) AS distinct_id_contrato,
               count(*) - count(DISTINCT id_contrato) AS extra_rows_for_repeated_ids,
               count(*) FILTER (WHERE id_contrato IS NULL) AS null_id_contrato
        FROM raw
    """)

    tracked_list = ", ".join(TRACKED)
    # A "version" = a distinct combination of values. 1 version = the copies are identical.
    con.sql(f"""
        CREATE TEMP TABLE dup AS
        WITH rep AS (
            SELECT * EXCLUDE (socrata_id) FROM raw
            WHERE id_contrato IN (SELECT id_contrato FROM raw GROUP BY 1 HAVING count(*) > 1)
        )
        SELECT id_contrato,
               count(*) AS copies,
               count(DISTINCT md5(CAST(row({tracked_list}) AS VARCHAR))) AS versions_tracked,
               count(DISTINCT md5(CAST(rep AS VARCHAR))) AS versions_all_columns
        FROM rep
        GROUP BY 1
    """)
    print("\nRepeated ids: are the copies identical (1 version) or different (>1)?")
    show(con, """
        SELECT copies, versions_tracked, versions_all_columns, count(*) AS ids
        FROM dup GROUP BY ALL ORDER BY ids DESC
    """)

    print("\nExamples of repeated ids whose tracked columns differ (what changes between copies?)")
    show(con, f"""
        SELECT r.id_contrato, r.socrata_id, {tracked_list}, r.ultima_actualizacion
        FROM raw r JOIN (SELECT id_contrato FROM dup WHERE versions_tracked > 1 LIMIT 4) d
          USING (id_contrato)
        ORDER BY r.id_contrato, r.socrata_id
    """, rows=20)

    # ------------------------------------------------------------------------------------------
    section("2. VALIDITY - values that do not convert (not null, but TRY_CAST is null)")
    parts = [
        f"""SELECT '{c}' AS col, 'decimal' AS target,
                   count({c}) AS not_null,
                   count(*) FILTER (WHERE {c} IS NOT NULL
                                    AND TRY_CAST({c} AS DECIMAL(38, 2)) IS NULL) AS invalid,
                   count(*) FILTER (WHERE TRY_CAST({c} AS DECIMAL(38, 2)) < 0) AS negative,
                   min(TRY_CAST({c} AS DECIMAL(38, 2)))::VARCHAR AS min,
                   max(TRY_CAST({c} AS DECIMAL(38, 2)))::VARCHAR AS max
            FROM raw"""
        for c in MONEY
    ]
    parts.append("""SELECT 'dias_adicionados', 'integer',
                   count(dias_adicionados),
                   count(*) FILTER (WHERE dias_adicionados IS NOT NULL
                                    AND TRY_CAST(dias_adicionados AS BIGINT) IS NULL),
                   count(*) FILTER (WHERE TRY_CAST(dias_adicionados AS BIGINT) < 0),
                   min(TRY_CAST(dias_adicionados AS BIGINT))::VARCHAR,
                   max(TRY_CAST(dias_adicionados AS BIGINT))::VARCHAR
            FROM raw""")
    parts += [
        f"""SELECT '{c}', 'timestamp',
                   count({c}),
                   count(*) FILTER (WHERE {c} IS NOT NULL AND TRY_CAST({c} AS TIMESTAMP) IS NULL),
                   NULL,
                   min(TRY_CAST({c} AS TIMESTAMP))::VARCHAR,
                   max(TRY_CAST({c} AS TIMESTAMP))::VARCHAR
            FROM raw"""
        for c in DATES
    ]
    show(con, " UNION ALL ".join(parts))

    print("\nHow raw values look (one example per column)")
    show(con, f"SELECT {', '.join(MONEY + DATES + ('dias_adicionados',))} FROM raw LIMIT 3")

    # ------------------------------------------------------------------------------------------
    section("3. CONSISTENCY - status values and business rules")
    print("All values of estado_contrato (exact text, with counts)")
    show(con, """
        SELECT estado_contrato, count(*) AS rows
        FROM raw GROUP BY 1 ORDER BY rows DESC
    """)

    show(con, """
        WITH t AS (
            SELECT TRY_CAST(valor_del_contrato AS DECIMAL(38, 2)) AS valor,
                   TRY_CAST(valor_pagado AS DECIMAL(38, 2)) AS pagado,
                   TRY_CAST(valor_facturado AS DECIMAL(38, 2)) AS facturado,
                   TRY_CAST(fecha_de_inicio_del_contrato AS TIMESTAMP) AS inicio,
                   TRY_CAST(fecha_de_fin_del_contrato AS TIMESTAMP) AS fin,
                   TRY_CAST(fecha_de_firma AS TIMESTAMP) AS firma
            FROM raw
        )
        SELECT count(*) FILTER (WHERE valor = 0)                AS contract_value_zero,
               count(*) FILTER (WHERE pagado > valor)           AS paid_more_than_value,
               count(*) FILTER (WHERE facturado > valor)        AS invoiced_more_than_value,
               count(*) FILTER (WHERE pagado > facturado)       AS paid_more_than_invoiced,
               count(*) FILTER (WHERE fin < inicio)             AS end_before_start,
               count(*) FILTER (WHERE firma > now())            AS signed_in_future,
               count(*) FILTER (WHERE firma < DATE '2025-01-01') AS signed_before_slice
        FROM t
    """)

    print("\nPlaceholder values in location columns")
    show(con, """
        SELECT 'ciudad' AS col, ciudad AS value, count(*) AS rows FROM raw
        WHERE ciudad ILIKE '%defin%' OR ciudad IS NULL GROUP BY ALL
        UNION ALL
        SELECT 'departamento', departamento, count(*) FROM raw
        WHERE departamento ILIKE '%defin%' OR departamento IS NULL GROUP BY ALL
    """)

    # ------------------------------------------------------------------------------------------
    section("4. COMPLETENESS - % of nulls per column (top 25)")
    show(con, """
        SELECT column_name, round(null_percentage, 2) AS null_pct
        FROM (SUMMARIZE raw)
        ORDER BY null_pct DESC
        LIMIT 25
    """, rows=25)

    # ------------------------------------------------------------------------------------------
    section("5. DISTRIBUTION - added days and categories")
    show(con, """
        WITH d AS (SELECT TRY_CAST(dias_adicionados AS BIGINT) AS dias FROM raw)
        SELECT count(*) FILTER (WHERE dias > 0)              AS contracts_with_added_days,
               round(100.0 * count(*) FILTER (WHERE dias > 0) / count(*), 2) AS pct,
               quantile_cont(dias, 0.5) FILTER (WHERE dias > 0)  AS p50_days,
               quantile_cont(dias, 0.95) FILTER (WHERE dias > 0) AS p95_days,
               max(dias) AS max_days
        FROM d
    """)
    for c in CATEGORIES:
        print(f"\n{c}: distinct values and top 8")
        show(con, f"""
            SELECT {c}, count(*) AS rows,
                   count(*) OVER () AS distinct_values
            FROM raw GROUP BY 1 ORDER BY rows DESC LIMIT 8
        """, rows=8)


if __name__ == "__main__":
    main()
