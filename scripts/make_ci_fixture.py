"""Write a tiny SYNTHETIC raw history for the end-to-end test in CI (no real data, no network).

Three collected days with a missed day between day 2 and day 3, written with the real raw writer
(staging -> publish, with a collector manifest), so silver, changes, dbt and the report run on it
exactly like on real data. Every case the pipeline has to handle appears at least once:

    day 1  2026-01-05 A, B, C, D + an identical copy of D (duplicate), E without id (quarantine)
    day 2  2026-01-06  A paid more (the source did NOT move ultima_actualizacion: an undated change)
                       B modified: status, +30 added days, later end date (dated by the source)
                       F is new
    (missed)  2026-01-07
    day 3  2026-01-08  A is "Terminado"; C disappeared

Suppliers: a company (NIT, its name may reach gold) and a person (Cédula, its name must not).

Usage: SECOP_DATA_DIR=ci-data uv run python scripts/make_ci_fixture.py
"""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime
from typing import Any

from secop_ledger.config import Settings
from secop_ledger.ingest import raw_writer
from secop_ledger.ingest.columns import SELECTED_COLUMNS

DAY_1, DAY_2, DAY_3 = date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8)


def contract(n: int, letter: str, **overrides: str | None) -> dict[str, Any]:
    """One raw row, all text like the API sends it. Fake values only."""
    company = n % 2 == 0
    row: dict[str, Any] = {
        ":id": f"row-{n:04d}",
        "id_contrato": f"CO1.PCCNTR.TEST{letter}",
        "codigo_entidad": "700000001" if n <= 3 else "700000002",
        "nit_entidad": "800000001" if n <= 3 else "800000002",
        "nombre_entidad": "ENTIDAD DE PRUEBA UNO" if n <= 3 else "ENTIDAD DE PRUEBA DOS",
        "departamento": "Cesar",
        "ciudad": "Valledupar",
        "orden": "Territorial",
        "sector": "Servicio Público",
        "codigo_proveedor": f"90000000{n}",
        "tipodocproveedor": "NIT" if company else "Cédula de Ciudadanía",
        "proveedor_adjudicado": "EMPRESA DE PRUEBA SAS" if company else "PERSONA DE PRUEBA",
        "es_pyme": "Si" if company else "No",
        "es_grupo": "No",
        "tipo_de_contrato": "Prestación de servicios",
        "modalidad_de_contratacion": "Contratación directa",
        "estado_contrato": "En ejecución",
        "fecha_de_firma": "2025-03-03T19:13:06.908",
        "fecha_de_inicio_del_contrato": "2025-03-04T00:00:00.000",
        "fecha_de_fin_del_contrato": "2025-12-31T00:00:00.000",
        "ultima_actualizacion": "2025-10-01T00:00:00.000",
        "valor_del_contrato": "1000000",
        "valor_facturado": "500000",
        "valor_pagado": "400000",
        "valor_pendiente_de_pago": "600000",
        "dias_adicionados": "0",
        "objeto_del_contrato": "Contrato de prueba",
    }
    row.update(overrides)
    return row


def day_rows() -> dict[date, list[dict[str, Any]]]:
    a, b, c, d, f = (contract(1, "A"), contract(2, "B"), contract(3, "C"),
                     contract(4, "D"), contract(6, "F"))
    a_paid = {**a, "valor_pagado": "600000", "valor_pendiente_de_pago": "400000"}
    b_modified = {
        **b,
        "estado_contrato": "Modificado",
        "dias_adicionados": "30",
        "fecha_de_fin_del_contrato": "2026-01-30T00:00:00.000",
        "ultima_actualizacion": "2026-01-05T00:00:00.000",
    }
    return {
        DAY_1: [a, b, c, d, {**d, ":id": "row-0099"}, contract(5, "E", id_contrato=None)],
        DAY_2: [a_paid, b_modified, c, d, f],
        DAY_3: [{**a_paid, "estado_contrato": "Terminado"}, b_modified, d, f],
    }


def main() -> int:
    data_dir = Settings.from_env().data_dir
    for day, rows in day_rows().items():
        started = datetime.now(UTC)
        written = raw_writer.write_staging([rows], SELECTED_COLUMNS, data_dir, day)
        raw_writer.publish(data_dir, day, {
            "ingest_date": day.isoformat(),
            "dataset_id": "synthetic",
            "filter": "synthetic fixture for CI",
            "rows": written,
            "source_count": written,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "seconds": 0.1,
            "columns": ["socrata_id", *SELECTED_COLUMNS],
        })
        print(f"{day}: {written} raw rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
