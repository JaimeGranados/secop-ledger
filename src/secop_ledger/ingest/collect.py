"""Daily collector: pull the full slice from SECOP II and store it as one raw snapshot.

Run:  uv run python -m secop_ledger.ingest.collect [--date YYYY-MM-DD] [--force]

Exit codes (so a scheduler or Airflow can see the failure):
    0  snapshot published, or already collected for that day
    1  the source schema changed (a column we need is missing)
    2  the row count does not match the source count
    3  any other error (network, disk...)
    4  the source reloaded during the download, on every try
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, date, datetime

from secop_ledger.config import DATASET_ID, Settings
from secop_ledger.ingest import raw_writer
from secop_ledger.ingest.columns import SELECTED_COLUMNS
from secop_ledger.ingest.keep_awake import keep_awake
from secop_ledger.ingest.socrata import SocrataClient, build_session

log = logging.getLogger("secop_ledger.collect")

# Rows can be added at the source while we read (a full pull takes minutes).
# A small difference is normal; a big one means pages were lost.
MAX_COUNT_DIFF_RATIO = 0.005  # 0.5 %

# A reload in the middle of the download makes the read invalid -> start again. Twice is enough:
# the source reloads about once a day, so two reloads in two downloads means something is wrong.
MAX_ATTEMPTS = 2


class SchemaDriftError(Exception):
    pass


class CountMismatchError(Exception):
    pass


class SourceRefreshedError(Exception):
    pass


def slice_filter(slice_start: date) -> str:
    return f"fecha_de_firma >= '{slice_start.isoformat()}T00:00:00'"


def check_count(expected: int, written: int) -> None:
    diff = abs(written - expected)
    if expected == 0 or diff / expected > MAX_COUNT_DIFF_RATIO:
        raise CountMismatchError(f"source count {expected}, written {written} (diff {diff})")
    if diff:
        log.warning(
            "source count %d, written %d (diff %d, within tolerance)", expected, written, diff
        )


def check_same_version(before: int, after: int) -> None:
    """Optimistic concurrency: the read is valid only if the source did not reload during it."""
    if before != after:
        raise SourceRefreshedError(
            f"source reloaded during the download (rowsUpdatedAt {before} -> {after}); "
            "row ids changed, so the pages mix two versions"
        )


def collect(
    settings: Settings,
    ingest_date: date,
    force: bool = False,
    client: SocrataClient | None = None,  # tests pass a fake
) -> int:
    final_dir = raw_writer.partition_dir(settings.data_dir, ingest_date)
    if final_dir.exists() and not force:
        log.info("%s already collected; nothing to do (use --force to replace it)", ingest_date)
        return 0

    client = client or SocrataClient(build_session(settings.app_token), DATASET_ID)
    started_at = datetime.now(UTC)
    t0 = time.monotonic()

    with keep_awake():
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                written, expected, version = _download(client, settings, ingest_date)
                break
            except SourceRefreshedError:
                if attempt == MAX_ATTEMPTS:
                    raise
                log.warning("source reloaded during the download; starting again (try %d of %d)",
                            attempt + 1, MAX_ATTEMPTS)

    raw_writer.publish(
        settings.data_dir,
        ingest_date,
        {
            "ingest_date": ingest_date.isoformat(),
            "dataset_id": DATASET_ID,
            "filter": slice_filter(settings.slice_start),
            "rows": written,
            "source_count": expected,
            "source_rows_updated_at": version,  # the source version we read (Unix seconds)
            "attempts": attempt,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "seconds": round(time.monotonic() - t0, 1),  # all tries together
            "columns": ["socrata_id", *SELECTED_COLUMNS],
        },
    )
    return 0


def _download(
    client: SocrataClient, settings: Settings, ingest_date: date
) -> tuple[int, int, int]:
    """One full read into staging. Returns (rows written, source count, source version)."""
    # 1. Contract check: stop before downloading anything if the source dropped a column.
    missing = set(SELECTED_COLUMNS) - client.column_names()
    if missing:
        raise SchemaDriftError(f"columns missing in the source: {sorted(missing)}")

    # 2. What the source says we should get, and which version of the source it is.
    where = slice_filter(settings.slice_start)
    version = client.rows_updated_at()
    expected = client.count(where)
    log.info("source has %d rows for %s (rowsUpdatedAt %d)", expected, where, version)

    # 3. Download into staging. 4. Reconcile. (5. Publish is done by the caller.)
    try:
        pages = client.iter_pages(SELECTED_COLUMNS, where, settings.page_size)
        written = raw_writer.write_staging(pages, SELECTED_COLUMNS, settings.data_dir, ingest_date)
        # The version first: a reload also breaks the count, but this says the real reason.
        check_same_version(version, client.rows_updated_at())
        check_count(expected, written)
    except BaseException:
        raw_writer.discard(settings.data_dir, ingest_date)
        raise
    return written, expected, version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat, default=date.today(),
                        help="ingestion date (default: today)")
    parser.add_argument("--force", action="store_true", help="replace an existing snapshot")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        return collect(Settings.from_env(), args.date, args.force)
    except SchemaDriftError:
        log.exception("schema drift")
        return 1
    except CountMismatchError:
        log.exception("reconciliation failed")
        return 2
    except SourceRefreshedError:
        log.exception("source reloaded during every try")
        return 4
    except Exception:
        log.exception("collector failed")
        return 3


if __name__ == "__main__":
    sys.exit(main())
