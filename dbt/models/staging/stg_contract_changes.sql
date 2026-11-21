-- One row per change event, with the tracked values ("after") as plain columns.
with source as (
    select * from {{ source('silver', 'contract_changes') }}
    where filename not like '%.tmp%'  -- never read a half-written day
)

select
    id_contrato,
    change_type,
    ingest_date,
    prev_ingest_date,
    -- Days with no snapshot between the two compared days. 2 = the change happened in one of
    -- the 3 days up to ingest_date; we only know the day we SAW it.
    case
        when change_type = 'changed' then datediff('day', prev_ingest_date, ingest_date) - 1
    end as gap_days,
    changed_columns,
    dated_by_source,
    "after".estado_contrato,
    "after".valor_del_contrato,
    "after".valor_facturado,
    "after".valor_pagado,
    "after".valor_pendiente_de_pago,
    "after".dias_adicionados,
    "after".fecha_de_fin_del_contrato,
    row_hash
from source
