"""Run the business questions in analysis/*.sql against the gold tables and write RESULTS.md.

Each SQL file starts with two comment lines:
    -- Title: <what the table shows>
    -- Note: <how to read it, what it can't show yet>   (the note may continue on more -- lines)

Usage (from the repo root, after `dbt build`):
    uv run python -m secop_ledger.report
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

ANALYSIS_DIR = Path("analysis")
RESULTS_FILE = ANALYSIS_DIR / "RESULTS.md"


def read_header(sql: str) -> tuple[str, str]:
    """Title and note from the leading `--` comment lines of a SQL file."""
    title, note_lines, in_note = "", [], False
    for line in sql.splitlines():
        if not line.startswith("--"):
            break
        text = line[2:].strip()
        if text.startswith("Title:"):
            title, in_note = text.removeprefix("Title:").strip(), False
        elif text.startswith("Note:"):
            note_lines, in_note = [text.removeprefix("Note:").strip()], True
        elif in_note:
            note_lines.append(text)
    return title, " ".join(note_lines)


def format_value(value: Any) -> str:
    """Numbers with thousands separators, so the README can copy them as they are."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float | Decimal):
        return f"{value:,.1f}"
    if isinstance(value, date):
        return value.isoformat()
    # Escape what Markdown would read as table syntax or formatting ("E.S.E.**" would turn bold).
    text = str(value)
    for char in ("\\", "|", "*", "_"):
        text = text.replace(char, "\\" + char)
    return text


def to_markdown(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    lines += ["| " + " | ".join(format_value(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def build_report(con: duckdb.DuckDBPyConnection, analysis_dir: Path) -> str:
    first_day, last_day, collected = con.sql(
        "SELECT min(ingest_date), max(ingest_date), count(*) FILTER (WHERE collected) "
        "FROM fct_daily_quality"
    ).fetchall()[0]
    parts = [
        "# Results of the business questions",
        "",
        f"Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC by `python -m secop_ledger.report` "
        "from the gold tables. **Do not edit by hand.**",
        "",
        f"History: **{first_day} → {last_day}**, {collected} days collected. "
        "Numbers that need a long history are marked in their note.",
    ]
    for sql_file in sorted(analysis_dir.glob("*.sql")):
        sql = sql_file.read_text(encoding="utf-8")
        title, note = read_header(sql)
        result = con.sql(sql)
        parts += [
            "",
            f"## {title or sql_file.stem}",
            "",
            f"_{note}_ (`analysis/{sql_file.name}`)" if note else f"`analysis/{sql_file.name}`",
            "",
            to_markdown(result.columns, result.fetchall()),
        ]
    return "\n".join(parts) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse",
        default=os.getenv("SECOP_WAREHOUSE_PATH", "data/warehouse.duckdb"),
        help="DuckDB file built by dbt",
    )
    args = parser.parse_args(argv)

    con = duckdb.connect(args.warehouse, read_only=True)  # the report never writes to gold
    report = build_report(con, ANALYSIS_DIR)
    RESULTS_FILE.write_text(report, encoding="utf-8")
    print(f"wrote {RESULTS_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
