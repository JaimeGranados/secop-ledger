"""A small client for the Socrata (SODA 2.x) API that datos.gov.co uses."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

BASE_URL = "https://www.datos.gov.co"
# (connect timeout, read timeout) in seconds. A big page can take about a minute to arrive.
TIMEOUT = (10, 300)

# Errors that can happen while the body of a response is arriving (e.g. "connection reset" in the
# middle of a 25,000-row page). urllib3's Retry does not cover them, so `_get` retries them itself.
# Safe because every request is a read: asking again for `:id > last_id` returns the same page.
BODY_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)
BODY_RETRIES = 4  # tries after the first one
BODY_BACKOFF_SECONDS = 30  # waits 30, 60, 90, 120 seconds


def build_session(app_token: str | None = None) -> requests.Session:
    """HTTP session that retries transient errors (rate limit, server errors) with backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,  # waits 2, 4, 8, 16... seconds between tries
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["Accept"] = "application/json"
    if app_token:
        session.headers["X-App-Token"] = app_token
    return session


class SocrataClient:
    def __init__(
        self,
        session: requests.Session,
        dataset_id: str,
        base_url: str = BASE_URL,
        sleep: Any = time.sleep,  # tests pass a fake, so they don't wait
    ):
        self.session = session
        self.dataset_id = dataset_id
        self.base_url = base_url
        self.sleep = sleep

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        for attempt in range(BODY_RETRIES + 1):
            try:
                response = self.session.get(
                    f"{self.base_url}{path}", params=params, timeout=TIMEOUT
                )
                response.raise_for_status()
                return response.json()  # the body is read here, so a broken body fails here
            except BODY_ERRORS as error:
                if attempt == BODY_RETRIES:
                    raise
                wait = BODY_BACKOFF_SECONDS * (attempt + 1)
                log.warning(
                    "request failed (%s), retry %d of %d in %d s",
                    type(error).__name__, attempt + 1, BODY_RETRIES, wait,
                )
                self.sleep(wait)
        raise AssertionError("unreachable")

    def column_names(self) -> set[str]:
        """Column names the dataset has today (from the dataset metadata)."""
        meta = self._get(f"/api/views/{self.dataset_id}.json")
        return {col["fieldName"] for col in meta["columns"]}

    def count(self, where: str) -> int:
        rows = self._get(
            f"/resource/{self.dataset_id}.json",
            {"$select": "count(*) AS n", "$where": where},
        )
        return int(rows[0]["n"])

    def iter_pages(
        self, columns: tuple[str, ...], where: str, page_size: int
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of rows using keyset pagination on the Socrata row id (`:id`).

        Why not `$offset`: if rows are added while we read, offset pages shift and we skip or
        repeat rows. Why not `id_contrato`: it is not unique in the source (repeated rows).
        `:id` is unique, so `:id > last_id` never skips or repeats a row.
        """
        last_id: str | None = None
        page_number = 0
        while True:
            clause = where if last_id is None else f"({where}) AND :id > '{last_id}'"
            rows = self._get(
                f"/resource/{self.dataset_id}.json",
                {
                    "$select": ",".join((":id", *columns)),
                    "$where": clause,
                    "$order": ":id",
                    "$limit": page_size,
                },
            )
            if not rows:
                return
            page_number += 1
            log.info("page %d: %d rows", page_number, len(rows))
            yield rows
            if len(rows) < page_size:
                return
            last_id = rows[-1][":id"]
