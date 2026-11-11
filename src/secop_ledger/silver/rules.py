"""Silver rules: column types, required columns, placeholders and quality thresholds.

Every rule here comes from the profile of day 1 (docs/data-model.md, section 4).
Thresholds live here, in one place, not hidden inside the SQL.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from secop_ledger.ingest.columns import SELECTED_COLUMNS

# --- Types ---------------------------------------------------------------------------------------
MONEY_COLUMNS: tuple[str, ...] = tuple(
    c
    for c in SELECTED_COLUMNS
    if c.startswith(("valor_", "saldo_", "presupuesto_", "sistema_", "recursos_"))
)
INTEGER_COLUMNS: tuple[str, ...] = ("dias_adicionados",)
DATE_COLUMNS: tuple[str, ...] = tuple(
    c for c in SELECTED_COLUMNS if c.startswith("fecha_") or c == "ultima_actualizacion"
)
# Everything else stays text. Ids and codes too: they are labels, not numbers
# (a code like "0123" would lose its leading zero as a number).
TEXT_COLUMNS: tuple[str, ...] = tuple(
    c for c in SELECTED_COLUMNS if c not in (*MONEY_COLUMNS, *INTEGER_COLUMNS, *DATE_COLUMNS)
)

SQL_TYPES = {
    **{c: "DECIMAL(38, 2)" for c in MONEY_COLUMNS},
    **{c: "BIGINT" for c in INTEGER_COLUMNS},
    **{c: "TIMESTAMP" for c in DATE_COLUMNS},
}

# --- Quarantine: a row without these can't be used -----------------------------------------------
REQUIRED_COLUMNS: tuple[str, ...] = (
    "id_contrato",
    "fecha_de_firma",
    "valor_del_contrato",
    "estado_contrato",
)

# --- Placeholders: text that means "unknown" → NULL (compared in lower case) --------------------
PLACEHOLDERS: tuple[str, ...] = ("no definido",)

# --- Change detection: the columns that open a new version (SCD Type 2) --------------------------
TRACKED_COLUMNS: tuple[str, ...] = (
    "estado_contrato",
    "valor_del_contrato",
    "valor_facturado",
    "valor_pagado",
    "valor_pendiente_de_pago",
    "dias_adicionados",
    "fecha_de_fin_del_contrato",
)

# --- Flags: usable but suspicious rows (kept in silver, never deleted) ---------------------------
# Biggest real contracts in Colombia (4G roads) are a few trillion COP. 10 trillion is far above.
MAX_CONTRACT_VALUE = Decimal("10000000000000")  # 10 billones COP
MAX_ADDED_DAYS = 3650  # 10 years
MIN_PLAUSIBLE_DATE = date(1990, 1, 1)
MAX_PLAUSIBLE_DATE = date(2100, 1, 1)
