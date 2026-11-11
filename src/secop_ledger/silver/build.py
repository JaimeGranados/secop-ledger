"""Build the silver layer for one or more days of raw snapshots.

Run:  uv run python -m secop_ledger.silver.build [--date YYYY-MM-DD | --all]
      (default: every raw day that has no silver yet)

For each day:
    raw (text) ──► typed + cleaned ──► contracts_snapshot   (one row per contract, flags, row_hash)
                                  └──► contracts_quarantine (unusable rows + reason)
    and the reconciliation must hold:  raw = snapshot + quarantine + duplicate copies removed

Exit codes:
    0  OK
    1  unknown contract status: add it to silver/status_map.csv and run again (raw keeps the day)
    2  reconciliation failed
    3  any other error
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from secop_ledger.config import Settings
from secop_ledger.ingest.columns import SELECTED_COLUMNS
from secop_ledger.silver import rules

log = logging.getLogger("secop_ledger.silver")

STATUS_MAP = Path(__file__).with_name("status_map.csv")
FLAGS = (
    "flag_value_zero",
    "flag_implausible_value",
    "flag_implausible_date",
    "flag_paid_over_value",
    "flag_invoiced_over_value",
    "flag_negative_pending",
    "flag_end_before_start",
)


class UnknownStatusError(Exception):
    pass


class ReconciliationError(Exception):
    pass


# --- Paths ---------------------------------------------------------------------------------------

def raw_dir(data_dir: Path, day: date) -> Path:
    return data_dir / "raw" / "contracts" / f"ingest_date={day.isoformat()}"


def silver_dir(data_dir: Path, table: str, day: date) -> Path:
    return data_dir / "silver" / table / f"ingest_date={day.isoformat()}"


def raw_days(data_dir: Path) -> list[date]:
    folders = (data_dir / "raw" / "contracts").glob("ingest_date=*")
    return sorted(
        date.fromisoformat(p.name.split("=")[1]) for p in folders if not p.name.endswith(".tmp")
    )


# --- SQL pieces (built from rules.py, so the SQL never hard-codes a column list) ----------------

def _q(name: str) -> str:
    return f'"{name}"'


def typed_select() -> str:
    """raw text → typed columns, placeholders → NULL, plus the list of quarantine reasons."""
    placeholders = ", ".join(f"'{p}'" for p in rules.PLACEHOLDERS)
    cols = []
    for c in SELECTED_COLUMNS:
        if c in rules.SQL_TYPES:
            cols.append(f"TRY_CAST(trim({_q(c)}) AS {rules.SQL_TYPES[c]}) AS {_q(c)}")
        else:
            cols.append(
                f"CASE WHEN lower(trim({_q(c)})) IN ({placeholders}) THEN NULL "
                f"ELSE nullif(trim({_q(c)}), '') END AS {_q(c)}"
            )

    reasons = [
        f"CASE WHEN nullif(trim({_q(c)}), '') IS NULL THEN 'missing_{c}' END"
        for c in rules.REQUIRED_COLUMNS
    ]
    reasons += [
        f"CASE WHEN nullif(trim({_q(c)}), '') IS NOT NULL "
        f"AND TRY_CAST(trim({_q(c)}) AS {t}) IS NULL THEN 'invalid_{c}' END"
        for c, t in rules.SQL_TYPES.items()
    ]
    reason_list = f"list_filter([{', '.join(reasons)}], lambda x: x IS NOT NULL) AS reasons"
    return f"SELECT socrata_id, {', '.join(cols)}, {reason_list} FROM src"


def row_hash_expr(columns: tuple[str, ...]) -> str:
    """md5 of the columns as text. NULL is written as a marker, so NULL and '' never collide."""
    parts = ", ".join(f"coalesce(CAST({_q(c)} AS VARCHAR), '<null>')" for c in columns)
    return f"md5(concat_ws('|', {parts}))"


def flags_select() -> str:
    lo, hi = rules.MIN_PLAUSIBLE_DATE.isoformat(), rules.MAX_PLAUSIBLE_DATE.isoformat()
    dates = ("fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato")
    out_of_range = " OR ".join(f"NOT ({d} BETWEEN DATE '{lo}' AND DATE '{hi}')" for d in dates)
    exprs = {
        "flag_value_zero": "valor_del_contrato = 0",
        "flag_implausible_value": f"valor_del_contrato > {rules.MAX_CONTRACT_VALUE} "
        f"OR dias_adicionados > {rules.MAX_ADDED_DAYS}",
        "flag_implausible_date": out_of_range,
        "flag_paid_over_value": "valor_pagado > valor_del_contrato",
        "flag_invoiced_over_value": "valor_facturado > valor_del_contrato",
        "flag_negative_pending": "valor_pendiente_de_pago < 0",
        "flag_end_before_start": "fecha_de_fin_del_contrato < fecha_de_inicio_del_contrato",
    }
    # A comparison with NULL gives NULL; coalesce turns "unknown" into "not flagged".
    return ", ".join(f"coalesce({e}, false) AS {name}" for name, e in exprs.items())


# --- Build one day -------------------------------------------------------------------------------

def build_day(data_dir: Path, day: date) -> dict[str, Any]:
    """Build silver for one day.

    Memory design: raw has 2M rows with long texts. The full rows are never copied into
    DuckDB tables; `src` and `typed` are views, read in a stream only when writing the outputs.
    The decisions (keep / duplicate / quarantine) live in one small table with no texts.
    """
    raw_files = (raw_dir(data_dir, day) / "*.parquet").as_posix()
    con = duckdb.connect()
    con.sql("SET preserve_insertion_order = false")  # lets DuckDB stream big COPY statements
    con.sql(f"SET temp_directory = '{(data_dir / '.duckdb_tmp').as_posix()}'")

    con.sql(f"CREATE VIEW src AS SELECT * FROM read_parquet('{raw_files}')")
    con.sql(f"CREATE VIEW typed AS {typed_select()}")
    con.sql(
        f"CREATE TEMP TABLE status_map AS "
        f"SELECT * FROM read_csv('{STATUS_MAP.as_posix()}', header = true, all_varchar = true)"
    )

    # Small table: one row per raw row, only what decisions need.
    con.sql("""
        CREATE TEMP TABLE checks AS
        SELECT socrata_id, id_contrato, estado_contrato, reasons, len(reasons) = 0 AS usable
        FROM typed
    """)

    # 1. Status guard: every status must be in the mapping table (before writing anything).
    unknown = con.sql("""
        SELECT DISTINCT estado_contrato FROM checks
        WHERE usable AND lower(estado_contrato) NOT IN (SELECT raw_key FROM status_map)
    """).fetchall()
    if unknown:
        raise UnknownStatusError(
            f"statuses not in {STATUS_MAP.name}: {sorted(r[0] for r in unknown)}"
        )

    # 2. Duplicates: compare ALL columns, but only for ids that repeat (a few hundred rows).
    all_cols = ", ".join(_q(c) for c in SELECTED_COLUMNS)
    con.sql(f"""
        CREATE TEMP TABLE conflicts AS
        SELECT id_contrato
        FROM typed
        WHERE len(reasons) = 0
          AND id_contrato IN (SELECT id_contrato FROM checks WHERE usable
                              GROUP BY 1 HAVING count(*) > 1)
        GROUP BY 1
        HAVING count(DISTINCT md5(CAST(row({all_cols}) AS VARCHAR))) > 1
    """)

    # 3. One decision per raw row. Among identical copies, the lowest socrata_id is kept.
    con.sql("""
        CREATE TEMP TABLE decision AS
        SELECT socrata_id, reasons,
               CASE
                   WHEN NOT usable THEN 'quarantine'
                   WHEN id_contrato IN (SELECT id_contrato FROM conflicts) THEN 'conflict'
                   WHEN row_number() OVER (PARTITION BY id_contrato, usable
                                           ORDER BY socrata_id) = 1 THEN 'keep'
                   ELSE 'duplicate'
               END AS decision
        FROM checks
    """)

    snap_tmp, snap_final = _staging(data_dir, "contracts_snapshot", day)
    quar_tmp, quar_final = _staging(data_dir, "contracts_quarantine", day)

    # 4. The snapshot: clean status, flags, and row_hash (on the CLEAN status, so a change from
    #    "terminado" to "Terminado" is not a change).
    con.sql(f"""
        COPY (
            SELECT *, {row_hash_expr(rules.TRACKED_COLUMNS)} AS row_hash
            FROM (
                SELECT t.* EXCLUDE (reasons, estado_contrato),
                       m.estado AS estado_contrato,
                       m.estado_grupo,
                       t.estado_contrato AS estado_contrato_raw,
                       {flags_select()}
                FROM typed t
                JOIN decision d USING (socrata_id)
                JOIN status_map m ON lower(t.estado_contrato) = m.raw_key
                WHERE d.decision = 'keep'
            )
        ) TO '{(snap_tmp / "part-00000.parquet").as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)

    # 5. Quarantine: the original raw row (as text) + why it was rejected.
    con.sql(f"""
        COPY (
            SELECT s.*,
                   CASE WHEN d.decision = 'conflict' THEN 'conflicting_duplicates'
                        ELSE array_to_string(d.reasons, ';') END AS quarantine_reason
            FROM src s JOIN decision d USING (socrata_id)
            WHERE d.decision IN ('quarantine', 'conflict')
        ) TO '{(quar_tmp / "part-00000.parquet").as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)

    # 6. Reconciliation on what was really WRITTEN: every raw row is explained.
    snap_file = (snap_tmp / "part-00000.parquet").as_posix()
    quar_file = (quar_tmp / "part-00000.parquet").as_posix()
    raw_rows, snapshot_rows, quarantine_rows, duplicates = con.sql(f"""
        SELECT (SELECT count(*) FROM src),
               (SELECT count(*) FROM read_parquet('{snap_file}')),
               (SELECT count(*) FROM read_parquet('{quar_file}')),
               (SELECT count(*) FROM decision WHERE decision = 'duplicate')
    """).fetchall()[0]
    if snapshot_rows + quarantine_rows + duplicates != raw_rows:
        for tmp in (snap_tmp, quar_tmp):
            shutil.rmtree(tmp, ignore_errors=True)
        raise ReconciliationError(
            f"raw {raw_rows} != snapshot {snapshot_rows} + quarantine {quarantine_rows} "
            f"+ duplicates removed {duplicates}"
        )

    flag_sums = ", ".join(f"count(*) FILTER (WHERE {f})" for f in FLAGS)
    flag_values = con.sql(f"SELECT {flag_sums} FROM read_parquet('{snap_file}')").fetchall()[0]
    reasons = dict(con.sql(f"""
        SELECT quarantine_reason, count(*) FROM read_parquet('{quar_file}')
        GROUP BY 1 ORDER BY 2 DESC, 1
    """).fetchall())

    manifest = {
        "ingest_date": day.isoformat(),
        "raw_rows": raw_rows,
        "snapshot_rows": snapshot_rows,
        "quarantine_rows": quarantine_rows,
        "duplicate_copies_removed": duplicates,
        "conflicting_duplicate_ids": con.sql("SELECT count(*) FROM conflicts").fetchall()[0][0],
        "quarantine_reasons": reasons,
        "flags": dict(zip(FLAGS, flag_values, strict=True)),
    }
    con.close()

    (snap_tmp / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    for tmp, final in ((snap_tmp, snap_final), (quar_tmp, quar_final)):
        if final.exists():
            shutil.rmtree(final)
        tmp.rename(final)
    log.info(
        "silver %s: %d raw = %d snapshot + %d quarantine + %d duplicates removed",
        day, raw_rows, snapshot_rows, quarantine_rows, duplicates,
    )
    return manifest


def _staging(data_dir: Path, table: str, day: date) -> tuple[Path, Path]:
    final = silver_dir(data_dir, table, day)
    tmp = final.with_name(final.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    return tmp, final


# --- CLI -----------------------------------------------------------------------------------------

def days_to_build(data_dir: Path, day: date | None, rebuild_all: bool) -> list[date]:
    if day:
        return [day]
    days = raw_days(data_dir)
    if rebuild_all:
        return days
    return [d for d in days if not silver_dir(data_dir, "contracts_snapshot", d).exists()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the silver layer from raw snapshots.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=date.fromisoformat, help="build (or rebuild) one day")
    group.add_argument("--all", action="store_true", help="rebuild every raw day")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    days = days_to_build(settings.data_dir, args.date, args.all)
    if not days:
        log.info("nothing to build: every raw day already has silver")
        return 0
    try:
        for d in days:
            build_day(settings.data_dir, d)
    except UnknownStatusError:
        log.exception("unknown status")
        return 1
    except ReconciliationError:
        log.exception("reconciliation failed")
        return 2
    except Exception:
        log.exception("silver build failed")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
