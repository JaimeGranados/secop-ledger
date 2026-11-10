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

### Headline number for each question
Each answer ends in **one sentence with a real, measured number** for the top of the README.
The `X` values are filled only after the pipeline has measured them. No number is estimated or invented.

| # | Headline (template) |
|---|---|
| Q1 | "X% of contracts signed in 2025 changed after signing; the top 10 entities account for Y% of all changes." |
| Q2 | "Contracts got X added days on average; in sector S it was Y days." |
| Q3 | "After 6 months, contracts had X% of their value paid (median)." |
| Q4 | "Contracts spend a median of X days in status S before they close." |
| Q5 | "The source sends X duplicate rows and Y undated changes per day; the pipeline counts none of them twice." |

### Pipeline numbers (also for the README)
Measured by the pipeline itself and stored in `fct_daily_quality` and the run logs:

- Rows processed per run, and total contracts tracked.
- Contract changes captured since the first run (history the source does not keep).
- Duplicate rows blocked, and rows sent to quarantine.
- Reconciliation: % of source rows explained (silver + quarantine = raw).
- Run time and cost per run ($0).
- Days of history collected without a gap.

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
**Version 1: a full snapshot of the slice every day**, then a row hash of the tracked columns
compared with the previous day's snapshot.

Why not an incremental pull with a watermark (`ultima_actualizacion > last run`)? Measured on
2026-11-10:

| Signing year | Contracts | With `ultima_actualizacion` |
|---|---|---|
| 2025 | 1,017,076 | 632,595 (62%) |
| 2026 | 952,664 | 339,087 (36%) |

About **half of the slice has no change date**, and the newest value was 5 days old. An incremental
pull would never see changes to those contracts. Only comparing full snapshots finds them.

Cost of the full pull: one 50,000-row page (all columns) took about 46 s, so the slice
(about 2M rows) takes roughly 20–30 min per day. That is fine for a daily batch.

**First real run (2026-11-10):** 1,969,740 rows written = 1,969,740 at the source (difference 0),
79 pages, 26.5 min, **526 MB** of Parquet (zstd). Two free-text columns are 56% of the size:
`objeto_del_contrato` (154 MB) and `descripcion_del_proceso` (138 MB). That is about 16 GB per
month, so raw needs a **retention policy**, decided with the silver layer: keep the first full
snapshot plus a daily table of new or changed rows only, and delete full snapshots after about
30 days.

Version 2 (an optimization, with a measured reason): add the incremental pull and compare it with
the full snapshot. The result ("the watermark alone misses X% of the changes") goes in the README.

## 4. Personal data

Rule: personal data is **removed at ingestion** (the API `$select` only asks for the columns we
need), so it never reaches the disk, the repo or the README.

| Columns | Decision |
|---|---|
| `nombre_*`, `tipo_de_documento_*`, `n_mero_de_documento_*` of the expense officer (ordenador del gasto), payment officer and supervisor | Not pulled |
| `nombre_representante_legal`, `identificaci_n_representante_legal`, `tipo_de_identificaci_n_representante_legal`, `g_nero_representante_legal`, `domicilio_representante_legal`, `nacionalidad_representante_legal` | Not pulled |
| `nombre_del_banco`, `tipo_de_cuenta`, `n_mero_de_cuenta` | Not pulled |
| `documento_proveedor` | Not pulled. `codigo_proveedor` (SECOP's own code) is the supplier key: on 2026-11-10 it was filled in 100% of the slice, with 732,852 distinct codes vs. 727,835 distinct documents (almost 1 to 1). So no hash of the personal ID is needed. (A plain hash of a 10-digit ID can be reversed by trying all numbers; if it is ever needed, use an HMAC with a secret key from `.env`.) |
| `proveedor_adjudicado` (supplier name) | Kept locally (it can be a person's name). The README shows results by **entity**, never by supplier name. |
