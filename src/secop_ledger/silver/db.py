"""One place to open DuckDB with limits that fit a small shared PC.

DuckDB's defaults are for a server: up to 80% of the RAM and every CPU thread. On a 12 GB desktop
that someone else is using, that freezes the PC for the 1-2 minutes of the silver build.
With a memory limit DuckDB writes what doesn't fit to `temp_directory` (spilling) instead:
a bit slower, but the PC stays usable. Both limits can be changed with environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DEFAULT_MEMORY_LIMIT = "2GB"
DEFAULT_THREADS = 2


def connect(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    memory_limit = os.getenv("SECOP_DUCKDB_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT)
    threads = int(os.getenv("SECOP_DUCKDB_THREADS", str(DEFAULT_THREADS)))
    con.sql(f"SET memory_limit = '{memory_limit}'")
    con.sql(f"SET threads = {threads}")
    con.sql(f"SET temp_directory = '{(data_dir / '.duckdb_tmp').as_posix()}'")
    con.sql("SET preserve_insertion_order = false")  # lets DuckDB stream big COPY statements
    return con
