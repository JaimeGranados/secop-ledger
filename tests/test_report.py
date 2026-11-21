"""Report tests: the header parser, number formatting, and a run on a tiny fake warehouse."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from secop_ledger.report import build_report, format_value, read_header, to_markdown


def test_header_reads_the_title_and_a_note_on_several_lines():
    sql = "-- Title: Q9 · Something\n-- Note: first part,\n-- second part.\nselect 1\n-- not header"
    assert read_header(sql) == ("Q9 · Something", "first part, second part.")


def test_numbers_are_formatted_for_the_readme():
    assert format_value(1969740) == "1,969,740"
    assert format_value(82.456) == "82.5"
    assert format_value(Decimal("30.0")) == "30.0"
    assert format_value(None) == "—"
    assert format_value(True) == "yes"
    assert format_value(date(2026, 11, 21)) == "2026-11-21"
    assert format_value("a|b") == "a\\|b"  # a pipe would break the markdown table
    assert format_value("E.S.E.**") == "E.S.E.\\*\\*"  # would turn bold


def test_markdown_table():
    assert to_markdown(["a", "b"], [(1, None)]) == "| a | b |\n|---|---|\n| 1 | — |"


def test_report_runs_every_sql_file_in_order(tmp_path):
    con = duckdb.connect()
    con.sql("""
        CREATE TABLE fct_daily_quality AS
        SELECT * FROM (VALUES (DATE '2026-11-10', true), (DATE '2026-11-11', false))
            AS t(ingest_date, collected)
    """)
    (tmp_path / "q2_b.sql").write_text("-- Title: Second\nselect 2 as n", encoding="utf-8")
    (tmp_path / "q1_a.sql").write_text(
        "-- Title: First\n-- Note: a note.\nselect 1000 as n", encoding="utf-8"
    )

    report = build_report(con, tmp_path)

    assert "History: **2026-11-10 → 2026-11-11**, 1 days collected." in report
    assert report.index("## First") < report.index("## Second")
    assert "_a note._ (`analysis/q1_a.sql`)" in report
    assert "| 1,000 |" in report
