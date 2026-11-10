"""Write one daily snapshot to the raw layer as Parquet.

Layout (Hive-style partitions, one folder per ingestion day):

    <data_dir>/raw/contracts/ingest_date=2026-11-10/part-00000.parquet
    <data_dir>/raw/contracts/ingest_date=2026-11-10/_manifest.json

Rules of the raw layer:
- Every value is stored as text, exactly as the API sent it. Types are set later, in silver.
  If the source sends a bad value one day, the raw load still works and nothing is lost.
- A day is written to a temporary folder first and renamed at the end. A run that crashes in the
  middle leaves no half-written partition that silver could read.
- Running the same day again replaces that day's partition (it never appends to it).
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

SOURCE_ID_COLUMN = "socrata_id"  # the API's `:id`, renamed (":" is awkward in SQL)


def raw_schema(columns: tuple[str, ...]) -> pa.Schema:
    return pa.schema([(SOURCE_ID_COLUMN, pa.string())] + [(c, pa.string()) for c in columns])


def _to_text(value: Any) -> str | None:
    """API values as text. Some columns (urlproceso) arrive as objects, e.g. {"url": "..."}."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def rows_to_table(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    # The API leaves out keys whose value is null, so every column is read with .get().
    data = {
        SOURCE_ID_COLUMN: [row.get(":id") for row in rows],
        **{
            name: [_to_text(row.get(name)) for row in rows]
            for name in schema.names
            if name != SOURCE_ID_COLUMN
        },
    }
    return pa.Table.from_pydict(data, schema=schema)


def partition_dir(data_dir: Path, ingest_date: date) -> Path:
    return data_dir / "raw" / "contracts" / f"ingest_date={ingest_date.isoformat()}"


def staging_dir(data_dir: Path, ingest_date: date) -> Path:
    final_dir = partition_dir(data_dir, ingest_date)
    return final_dir.with_name(final_dir.name + ".tmp")


def write_staging(
    pages: Iterable[list[dict[str, Any]]],
    columns: tuple[str, ...],
    data_dir: Path,
    ingest_date: date,
) -> int:
    """Stream pages into the day's staging folder and return the number of rows written.

    Pages are written one by one, so memory use stays at about one page, not the whole day.
    Nothing is visible to the next layers until `publish()` is called.
    """
    tmp_dir = staging_dir(data_dir, ingest_date)
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)  # left by a run that crashed
    tmp_dir.mkdir(parents=True)

    schema = raw_schema(columns)
    rows_written = 0
    with pq.ParquetWriter(tmp_dir / "part-00000.parquet", schema, compression="zstd") as writer:
        for page in pages:
            writer.write_table(rows_to_table(page, schema))
            rows_written += len(page)
    return rows_written


def publish(data_dir: Path, ingest_date: date, manifest: dict[str, Any]) -> Path:
    """Write the manifest and move the staging folder to its final place (replacing that day)."""
    tmp_dir = staging_dir(data_dir, ingest_date)
    final_dir = partition_dir(data_dir, ingest_date)
    (tmp_dir / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if final_dir.exists():
        shutil.rmtree(final_dir)
    tmp_dir.rename(final_dir)
    log.info("published %s (%s rows)", final_dir, manifest.get("rows"))
    return final_dir


def discard(data_dir: Path, ingest_date: date) -> None:
    shutil.rmtree(staging_dir(data_dir, ingest_date), ignore_errors=True)
