# Data model

This document defines **what the pipeline must answer** and **what one row means in every table**
(the grain). Everything else (columns to pull, tables, tests) follows from it.

## 1. Business questions

The pipeline exists to answer these five questions. Each one is a SQL query in `analysis/` with its
result in the README.

| # | Question | Main columns | Answered by |
|---|---|---|---|
| Q1 | Which public entities change their contracts most after signing? | `estado_contrato`, `valor_del_contrato`, `dias_adicionados`, `fecha_de_fin_del_contrato` | `contract_history` + `dim_entity` |
| Q2 | How many days are added to contracts, by sector and contract type? | `dias_adicionados`, `sector`, `tipo_de_contrato` | `contract_history` (current version) |
| Q3 | How fast do entities pay? (paid vs. contract value over the life of a contract) | `valor_del_contrato`, `valor_facturado`, `valor_pagado` | `contract_history` |
| Q4 | Which statuses do contracts move through, and how long do they stay in each one? | `estado_contrato`, `ultima_actualizacion` | `fct_contract_changes` |
| Q5 | How dirty is the source every day? (duplicates, rejected rows, changes the source did not date) | all | `fct_daily_quality` |

> **What the data does not prove.** A modified contract or added days are normal in public
> contracting. These numbers describe facts; they are not evidence that anything was done wrong.

## 2. Scope (the slice)

Contracts with `fecha_de_firma >= 2025-01-01` (about 2M rows on 2026-11-10). Changes to those
contracts are tracked forever, also after 2025.

## 3. Tables and grain

| Layer | Table | Grain: one row = | Key |
|---|---|---|---|
| raw | `raw/contracts/ingest_date=YYYY-MM-DD/*.parquet` | one record **as the API returned it** on one ingestion day (may contain duplicates) | none (append only) |
| silver | `contracts_snapshot` | one contract on one ingestion day | `id_contrato`, `ingest_date` |
| silver | `contracts_quarantine` | one rejected raw record on one ingestion day, with the reason | `ingest_date`, `raw_row_number` |
| gold | `dim_entity` | one public entity (current values, SCD Type 1) | `entity_key` (from `codigo_entidad`) |
| gold | `dim_supplier` | one supplier (current values, SCD Type 1) | `supplier_key` (from `codigo_proveedor`) |
| gold | `dim_date` | one calendar day | `date_key` |
| gold | `contract_history` | one **version** of one contract: a period where its tracked columns did not change (SCD Type 2) | `contract_version_key`; `valid_from`, `valid_to`, `is_current` |
| gold | `fct_contract_changes` | one detected change of one contract (old value → new value) | `id_contrato`, `changed_on`, `column_name` |
| gold | `fct_daily_quality` | one ingestion day: rows received, duplicates, rejected, changed, new | `ingest_date` |

### Tracked columns (they open a new version in `contract_history`)
`estado_contrato`, `valor_del_contrato`, `valor_facturado`, `valor_pagado`,
`valor_pendiente_de_pago`, `dias_adicionados`, `fecha_de_fin_del_contrato`.

A change in any other column (for example a typo fixed in the description) updates the snapshot but
does **not** open a new version. Reason: versions must mean a business change, not noise.

### Change detection
1. **Incremental pull:** records with `ultima_actualizacion > last watermark - 7 days`.
   The 7-day lookback covers the source lag (on 2026-11-10 the newest value was 2026-11-05).
2. **Row hash:** a hash of the tracked columns, compared with the previous snapshot. It catches
   changes the source did not date (`ultima_actualizacion` is empty in about 38% of rows).
3. **Weekly full pull** of the slice, to reconcile counts and find deleted records.

## 4. Personal data

Rule: personal data is **removed at ingestion** (the API `$select` only asks for the columns we
need), so it never reaches the disk, the repo or the README.

| Columns | Decision |
|---|---|
| `nombre_*`, `tipo_de_documento_*`, `n_mero_de_documento_*` of the expense officer (ordenador del gasto), payment officer and supervisor | Not pulled |
| `nombre_representante_legal`, `identificaci_n_representante_legal`, `tipo_de_identificaci_n_representante_legal`, `g_nero_representante_legal`, `domicilio_representante_legal`, `nacionalidad_representante_legal` | Not pulled |
| `nombre_del_banco`, `tipo_de_cuenta`, `n_mero_de_cuenta` | Not pulled |
| `documento_proveedor` | Not pulled; `codigo_proveedor` (SECOP's own code) is the supplier key. *To confirm in the spike: it is filled and unique per supplier.* If it is not, use an HMAC of `documento_proveedor` with a secret key from `.env` (a plain hash of a 10-digit ID can be reversed by trying all numbers). |
| `proveedor_adjudicado` (supplier name) | Kept locally (it can be a person's name). The README shows results by **entity**, never by supplier name. |
