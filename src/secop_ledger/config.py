"""Settings read from environment variables (and from .env, if it exists)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

DATASET_ID = "jbjy-vk9h"  # SECOP II - Contratos Electronicos


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    slice_start: date
    page_size: int
    app_token: str | None

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv()
        return cls(
            data_dir=Path(os.getenv("SECOP_DATA_DIR", "data")),
            slice_start=date.fromisoformat(os.getenv("SECOP_SLICE_START", "2025-01-01")),
            page_size=int(os.getenv("SECOP_PAGE_SIZE", "25000")),
            app_token=os.getenv("SOCRATA_APP_TOKEN") or None,
        )
