-- SCD Type 2: one row per version of a contract (a version = the 7 tracked values).
--
-- Built from the change events, not from the full snapshots: baseline / new / changed each open
-- a version, and the next event of the same contract (changed or disappeared) closes it.
-- valid_from = the day we SAW the version. After a gap it may have started up to gap_days earlier.
with events as (
    select
        *,
        lead(ingest_date) over (
            partition by id_contrato order by ingest_date
        ) as next_event_date
    from {{ ref('stg_contract_changes') }}
),

versions as (
    select * from events
    where change_type <> 'disappeared'  -- a disappearance only closes the previous version
)

select
    id_contrato || '@' || strftime(ingest_date, '%Y-%m-%d') as version_key,
    id_contrato,
    row_number() over (partition by id_contrato order by ingest_date) as version_number,
    ingest_date as valid_from,
    next_event_date as valid_to,  -- exclusive; null = still open
    next_event_date is null as is_current,
    change_type as opened_by,  -- baseline = first seen on day 1, not the real start
    changed_columns,
    gap_days,
    dated_by_source,
    estado_contrato,
    valor_del_contrato,
    valor_facturado,
    valor_pagado,
    valor_pendiente_de_pago,
    dias_adicionados,
    fecha_de_fin_del_contrato,
    row_hash
from versions
