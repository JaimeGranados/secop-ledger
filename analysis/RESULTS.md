# Results of the business questions

Generated 2026-11-21 22:41 UTC by `python -m secop_ledger.report` from the gold tables. **Do not edit by hand.**

History: **2026-11-10 → 2026-11-21**, 6 days collected. Numbers that need a long history are marked in their note.

## Q1 · Share of contracts modified after signing, by signing year

_"Modified" = the terms changed after signing: current status "Modificado", or added days (both recorded by the source since signing), or a change of contract value, added days or end date that the pipeline observed itself. Payments moving is not a modification._ (`analysis/q1_1_modified_share.sql`)

| signing_year | contracts | modified | pct_modified |
|---|---|---|---|
| 2025 | 1,017,004 | 262,065 | 25.8 |
| 2026 | 993,302 | 326,691 | 32.9 |

## Q1 · Entities that modify the largest share of their contracts (entities with 1,000+ contracts)

_Same definition of "modified" as Q1 · share. A high share is a fact, not evidence of wrongdoing: extensions and value changes are normal in public contracting._ (`analysis/q1_2_top_entities.sql`)

| entity | sector | contracts | modified | pct_modified |
|---|---|---|---|---|
| SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (OFICIAL) | Salud y Protección Social | 12,725 | 11,074 | 87.0 |
| SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR E.S.E.\*\* | Salud y Protección Social | 13,540 | 11,590 | 85.6 |
| SUBRED INTEGRADA DE SERVICIO DE SALUD CENTRO ORIENTE E.S.E 1 | Salud y Protección Social | 11,775 | 9,897 | 84.1 |
| SUBRED INTEGRADA DE SERVICIOS DE SALUD SUR OCCIDENTE ESE. | Salud y Protección Social | 13,439 | 10,754 | 80.0 |
| ALCALDIA LOCAL DE USAQUEN | Servicio Público | 1,135 | 728 | 64.1 |
| ICBF REGIONAL MAGDALENA | Salud y Protección Social | 1,532 | 954 | 62.3 |
| ICBF REGIONAL SANTANDER | Salud y Protección Social | 1,876 | 1,163 | 62.0 |
| ALCALDIA MUNICIPAL DE GIRARDOT | Servicio Público | 2,828 | 1,741 | 61.6 |
| ALCALDIA LOCAL DE SAN CRISTOBAL | Servicio Público | 1,934 | 1,173 | 60.7 |
| DEPARTAMENTO NACIONAL DE PLANEACIÓN | Planeación | 3,242 | 1,960 | 60.5 |

## Q1 · How concentrated the modified contracts are in a few entities

_Share of all modified contracts that belong to the 10 entities with the most modified contracts._ (`analysis/q1_3_concentration.sql`)

| entities_with_modified_contracts | modified_contracts | modified_in_top_10 | pct_in_top_10 |
|---|---|---|---|
| 3,358 | 588,756 | 86,922 | 14.8 |

## Q2 · Added days, all contracts

_dias_adicionados is cumulative since signing (recorded by the source), current version. Values over 3,650 days (10 years) are impossible and are left out of the median; they are counted._ (`analysis/q2_1_added_days_overall.sql`)

| contracts | with_added_days | pct_with_added_days | median_added_days | avg_added_days | excluded_over_10_years |
|---|---|---|---|---|---|
| 2,010,306 | 459,495 | 22.9 | 60.0 | 64.1 | 45 |

## Q2 · Added days by sector (the 10 sectors with the most contracts)

_Median over contracts that got added days (1 to 3,650)._ (`analysis/q2_2_added_days_by_sector.sql`)

| sector | contracts | pct_with_added_days | median_added_days |
|---|---|---|---|
| Servicio Público | 566,686 | 21.8 | 60.0 |
| No aplica/No pertenece | 350,590 | 21.0 | 59.0 |
| Salud y Protección Social | 300,849 | 30.1 | 60.0 |
| Educación Nacional | 114,768 | 19.5 | 30.0 |
| Trabajo | 84,507 | 18.3 | 21.0 |
| deportes | 81,606 | 17.9 | 48.0 |
| Ambiente y Desarrollo Sostenible | 76,191 | 23.0 | 46.0 |
| Inclusión Social y Reconciliación | 54,486 | 29.8 | 77.0 |
| defensa | 53,508 | 22.4 | 72.0 |
| Cultura | 46,322 | 26.0 | 51.0 |

## Q2 · Added days by contract type (types with 1,000+ contracts)

_Median over contracts that got added days (1 to 3,650)._ (`analysis/q2_3_added_days_by_type.sql`)

| contract_type | contracts | pct_with_added_days | median_added_days |
|---|---|---|---|
| Interventoría | 3,446 | 46.3 | 86.0 |
| Obra | 17,203 | 36.0 | 60.0 |
| Consultoría | 3,513 | 30.8 | 60.0 |
| Otro | 61,974 | 25.2 | 61.0 |
| Prestación de servicios | 1,752,225 | 23.4 | 60.0 |
| Compraventa | 27,432 | 19.0 | 29.0 |
| Arrendamiento de inmuebles | 12,858 | 16.1 | 92.0 |
| Seguros | 3,849 | 15.4 | 62.0 |
| Suministros | 42,722 | 14.9 | 40.0 |
| Decreto 092 de 2017 | 79,790 | 13.1 | 60.0 |
| Comodato | 3,094 | 8.9 | 48.0 |

## Q3 · Share of the contract value paid, by months since signing

_A cross-section on the latest collected day (each contract once, grouped by its age), not yet one contract followed over time: that needs months of history. Only contracts with value > 0; contracts with paid > value (a quality flag) are left out and counted. "Any payment" = the entity reported a payment in SECOP; many entities report payments late or never._ (`analysis/q3_1_paid_by_age.sql`)

| age_since_signing | contracts | pct_with_any_payment | median_pct_paid | excluded_paid_over_value |
|---|---|---|---|---|
| 0: less than 1 month | 42,284 | 0.1 | 0.0 | 0 |
| 1: 1-2 months | 168,377 | 14.2 | 0.0 | 0 |
| 2: 3-5 months | 222,288 | 38.9 | 0.0 | 2 |
| 3: 6-11 months | 605,292 | 59.6 | 62.5 | 65 |
| 4: 12+ months | 961,021 | 60.4 | 87.5 | 542 |

## Q4 · Where contracts are now (current status)

_"How long contracts stay in each status" needs a status to start AND end inside our history; with a few days of history that would only measure very short stays. Measured later._ (`analysis/q4_1_current_status.sql`)

| status | contracts | pct |
|---|---|---|
| En ejecución | 959,542 | 47.7 |
| Modificado | 507,396 | 25.2 |
| Cerrado | 297,407 | 14.8 |
| Terminado | 193,380 | 9.6 |
| Aprobado | 35,688 | 1.8 |
| Cedido | 13,532 | 0.7 |
| Suspendido | 3,361 | 0.2 |

## Q4 · Status moves observed by the pipeline (top 10)

_Only moves seen since the first run. After a missed day, two moves can look like one._ (`analysis/q4_2_status_moves.sql`)

| from_status | to_status | moves | pct_of_moves |
|---|---|---|---|
| Aprobado | En ejecución | 26,802 | 39.0 |
| En ejecución | Modificado | 21,354 | 31.1 |
| En ejecución | Cerrado | 5,005 | 7.3 |
| En ejecución | Terminado | 4,009 | 5.8 |
| Modificado | Cerrado | 3,330 | 4.8 |
| Modificado | Terminado | 3,013 | 4.4 |
| Terminado | Cerrado | 1,767 | 2.6 |
| En ejecución | Suspendido | 584 | 0.9 |
| Aprobado | Modificado | 525 | 0.8 |
| Suspendido | En ejecución | 461 | 0.7 |

## Q5 · How dirty the source is (summary of the collected days)

_Every number comes from fct_daily_quality, where every collected day is reconciled (raw = source; raw = snapshot + quarantine + duplicates removed) by dbt tests. Duplicates are counted per day: the same 111 copies came on 11-10 and 11-11, then the source stopped sending them._ (`analysis/q5_1_quality_summary.sql`)

| calendar_days | days_collected | days_missed | duplicate_copies_removed | rows_quarantined | days_not_reconciled | changed_contracts | changed_not_dated_by_source | pct_changes_not_dated_by_source |
|---|---|---|---|---|---|---|---|---|
| 12 | 6 | 6 | 222 | 0 | 0 | 293,601 | 244,569 | 83.3 |

## Q5 · Quality by day

_Missed days have no numbers (collected = false). Flags are counted on the snapshot of that day._ (`analysis/q5_2_quality_by_day.sql`)

| day | collected | source_rows | duplicates | quarantined | new | changed | pct_not_dated | value_zero | paid_over_value | end_before_start |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | yes | 1,969,740 | 111 | 0 | 0 | 0 | — | 10,182 | 623 | 618 |
| 2026-11-11 | yes | 1,969,760 | 111 | 0 | 20 | 8,044 | 84.0 | 10,182 | 623 | 618 |
| 2026-11-12 | yes | 1,980,863 | 0 | 0 | 11,214 | 60,635 | 90.8 | 10,257 | 622 | 618 |
| 2026-11-13 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-14 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-15 | yes | 1,997,605 | 0 | 0 | 16,742 | 103,073 | 82.3 | 10,354 | 616 | 616 |
| 2026-11-16 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-17 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-18 | yes | 2,000,707 | 0 | 0 | 3,102 | 15,194 | 49.4 | 10,374 | 616 | 616 |
| 2026-11-19 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-20 | no | — | — | — | — | — | — | — | — | — |
| 2026-11-21 | yes | 2,010,306 | 0 | 0 | 9,599 | 106,655 | 84.8 | 10,435 | 610 | 613 |
