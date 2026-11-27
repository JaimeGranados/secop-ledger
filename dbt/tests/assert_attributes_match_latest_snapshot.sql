-- Reconciliation of the attributes DELTA: rebuilt from the delta, the current full rows must be
-- exactly the newest snapshot. For every contract in the newest snapshot, its newest attribute
-- row must exist and have the same full_hash (a hash of every column except socrata_id).
-- If this passes, storing full rows only when they change loses nothing.
with latest_day as (
    select max(ingest_date) as day from {{ ref('stg_contract_changes') }}
),

snapshot as (
    select id_contrato, full_hash
    from {{ source('silver', 'contracts_snapshot') }}
    where filename not like '%.tmp%'
      and ingest_date = (select day from latest_day)
),

newest_attributes as (
    select id_contrato, full_hash
    from {{ source('silver', 'contracts_attributes') }}
    where filename not like '%.tmp%'
    qualify ingest_date = max(ingest_date) over (partition by id_contrato)
)

select
    s.id_contrato,
    s.full_hash as snapshot_hash,
    a.full_hash as attributes_hash
from snapshot s
left join newest_attributes a on a.id_contrato = s.id_contrato
where a.full_hash is distinct from s.full_hash
