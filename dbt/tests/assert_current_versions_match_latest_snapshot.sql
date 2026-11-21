-- Reconciliation: the history rebuilt from the events must give back the latest snapshot.
-- current versions = contracts in the last snapshot, and the same row_hash for each one.
with latest_day as (
    select max(ingest_date) as day from {{ ref('stg_contract_changes') }}
),

snapshot as (
    select id_contrato, row_hash
    from {{ source('silver', 'contracts_snapshot') }}
    where filename not like '%.tmp%'
      and ingest_date = (select day from latest_day)
),

current_versions as (
    select id_contrato, row_hash
    from {{ ref('contract_history') }}
    where is_current
)

select
    coalesce(s.id_contrato, c.id_contrato) as id_contrato,
    s.row_hash as snapshot_hash,
    c.row_hash as history_hash
from snapshot s
full outer join current_versions c on s.id_contrato = c.id_contrato
where s.row_hash is distinct from c.row_hash
