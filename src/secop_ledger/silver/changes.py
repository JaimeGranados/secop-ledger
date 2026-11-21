"""Change detection: compare each silver snapshot with the previous one.

Run:  uv run python -m secop_ledger.silver.changes [--date YYYY-MM-DD | --all]
      (default: every silver day that has no changes yet, oldest first)

Output: silver/contract_changes/ingest_date=YYYY-MM-DD/part-00000.parquet
One row per contract that is new, changed or gone (unchanged contracts are not stored):

    change_type   baseline     first snapshot ever: every contract (the starting photo)
                  new          in today's snapshot, not in the previous one
                  changed      in both, but row_hash (7 tracked columns) is different
                  disappeared  in the previous snapshot, not today
    before/after  the 7 tracked columns, as structs
    changed_columns           which tracked columns changed
    dated_by_source           for "changed": did the source also update ultima_actualizacion?

Baseline + every later event = the full history. Old full snapshots are not needed to rebuild it.

Note: if a day is missing (a gap), the changes are measured against the last snapshot we have,
and `prev_ingest_date` says which one. We know THAT it changed in the gap, not on which day.

Exit codes: 0 OK · 2 reconciliation failed · 3 any other error
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
from secop_ledger.silver import rules
from secop_ledger.silver.build import ReconciliationError, silver_dir

log = logging.getLogger("secop_ledger.changes")

TABLE = "contract_changes"


def snapshot_days(data_dir: Path) -> list[date]:
    folders = (data_dir / "silver" / "contracts_snapshot").glob("ingest_date=*")
    return sorted(
        date.fromisoformat(p.name.split("=")[1]) for p in folders if not p.name.endswith(".tmp")
    )


def _snapshot_sql(data_dir: Path, day: date) -> str:
    cols = ", ".join(("id_contrato", "row_hash", "ultima_actualizacion", *rules.TRACKED_COLUMNS))
    path = (silver_dir(data_dir, "contracts_snapshot", day) / "*.parquet").as_posix()
    return f"SELECT {cols} FROM read_parquet('{path}')"


def _struct(alias: str) -> str:
    fields = ", ".join(f"{c} := {alias}.{c}" for c in rules.TRACKED_COLUMNS)
    return f"struct_pack({fields})"


def build_changes(data_dir: Path, day: date) -> dict[str, Any]:
    days = snapshot_days(data_dir)
    if day not in days:
        raise FileNotFoundError(f"no silver snapshot for {day}")
    prev = max((d for d in days if d < day), default=None)

    con = duckdb.connect()
    con.sql(f"CREATE VIEW cur AS {_snapshot_sql(data_dir, day)}")
    if prev is None:
        con.sql("CREATE VIEW prev AS SELECT * FROM cur WHERE false")  # same columns, no rows
    else:
        con.sql(f"CREATE VIEW prev AS {_snapshot_sql(data_dir, prev)}")

    changed_cols = ", ".join(
        f"CASE WHEN c.{col} IS DISTINCT FROM p.{col} THEN '{col}' END"
        for col in rules.TRACKED_COLUMNS
    )
    new_label = "'baseline'" if prev is None else "'new'"
    prev_value = f"DATE '{prev.isoformat()}'" if prev else "CAST(NULL AS DATE)"

    final = silver_dir(data_dir, TABLE, day)
    tmp = final.with_name(final.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    out_file = (tmp / "part-00000.parquet").as_posix()

    con.sql(f"""
        COPY (
            SELECT coalesce(c.id_contrato, p.id_contrato) AS id_contrato,
                   CASE WHEN p.id_contrato IS NULL THEN {new_label}
                        WHEN c.id_contrato IS NULL THEN 'disappeared'
                        ELSE 'changed' END AS change_type,
                   DATE '{day.isoformat()}' AS ingest_date,
                   {prev_value} AS prev_ingest_date,
                   CASE WHEN c.id_contrato IS NOT NULL AND p.id_contrato IS NOT NULL
                        THEN list_filter([{changed_cols}], lambda x: x IS NOT NULL)
                   END AS changed_columns,
                   CASE WHEN p.id_contrato IS NOT NULL THEN {_struct("p")} END AS before,
                   CASE WHEN c.id_contrato IS NOT NULL THEN {_struct("c")} END AS after,
                   p.row_hash AS prev_row_hash,
                   c.row_hash AS row_hash,
                   CASE WHEN c.id_contrato IS NOT NULL AND p.id_contrato IS NOT NULL
                        THEN c.ultima_actualizacion IS DISTINCT FROM p.ultima_actualizacion
                   END AS dated_by_source
            FROM cur c
            FULL OUTER JOIN prev p ON c.id_contrato = p.id_contrato
            WHERE p.id_contrato IS NULL
               OR c.id_contrato IS NULL
               OR c.row_hash <> p.row_hash
        ) TO '{out_file}' (FORMAT parquet, COMPRESSION zstd)
    """)

    counts = dict(con.sql(f"""
        SELECT change_type, count(*) FROM read_parquet('{out_file}') GROUP BY 1
    """).fetchall())
    cur_rows, prev_rows = con.sql(
        "SELECT (SELECT count(*) FROM cur), (SELECT count(*) FROM prev)"
    ).fetchall()[0]
    added = counts.get("new", 0) + counts.get("baseline", 0)
    gone = counts.get("disappeared", 0)

    # Reconciliation: today = previous + new - disappeared.
    if cur_rows != prev_rows + added - gone:
        shutil.rmtree(tmp, ignore_errors=True)
        raise ReconciliationError(
            f"today {cur_rows} != previous {prev_rows} + new {added} - disappeared {gone}"
        )

    dated = dict(con.sql(f"""
        SELECT dated_by_source, count(*) FROM read_parquet('{out_file}')
        WHERE change_type = 'changed' GROUP BY 1
    """).fetchall())
    by_column = dict(con.sql(f"""
        SELECT col, count(*) FROM (
            SELECT unnest(changed_columns) AS col FROM read_parquet('{out_file}')
            WHERE change_type = 'changed'
        ) GROUP BY 1 ORDER BY 2 DESC, 1
    """).fetchall())
    con.close()

    manifest = {
        "ingest_date": day.isoformat(),
        "prev_ingest_date": prev.isoformat() if prev else None,
        "gap_days": (day - prev).days - 1 if prev else None,
        "snapshot_rows": cur_rows,
        "prev_snapshot_rows": prev_rows,
        "baseline": counts.get("baseline", 0),
        "new": counts.get("new", 0),
        "changed": counts.get("changed", 0),
        "disappeared": gone,
        "changed_dated_by_source": dated.get(True, 0),
        "changed_not_dated_by_source": dated.get(False, 0),
        "changed_by_column": by_column,
    }
    (tmp / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if final.exists():
        shutil.rmtree(final)
    tmp.rename(final)
    log.info(
        "changes %s vs %s: %d baseline, %d new, %d changed, %d disappeared",
        day, prev, manifest["baseline"], manifest["new"], manifest["changed"], gone,
    )
    return manifest


def days_to_build(data_dir: Path, day: date | None, rebuild_all: bool) -> list[date]:
    if day:
        return [day]
    days = snapshot_days(data_dir)
    if rebuild_all:
        return days
    return [d for d in days if not silver_dir(data_dir, TABLE, d).exists()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect contract changes between snapshots.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=date.fromisoformat, help="build (or rebuild) one day")
    group.add_argument("--all", action="store_true", help="rebuild every day, oldest first")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    days = days_to_build(settings.data_dir, args.date, args.all)
    if not days:
        log.info("nothing to build: every silver day already has changes")
        return 0
    try:
        for d in days:  # oldest first: each day depends on the one before
            build_changes(settings.data_dir, d)
    except ReconciliationError:
        log.exception("reconciliation failed")
        return 2
    except Exception:
        log.exception("change detection failed")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
