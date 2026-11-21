-- Reconciliation: dim_contract has exactly the contracts of contract_history (none missing, none extra).
with history as (select distinct id_contrato from {{ ref('contract_history') }}),
dim as (select id_contrato from {{ ref('dim_contract') }})

select coalesce(h.id_contrato, d.id_contrato) as id_contrato,
       h.id_contrato is null as only_in_dim,
       d.id_contrato is null as only_in_history
from history h
full outer join dim d on h.id_contrato = d.id_contrato
where h.id_contrato is null or d.id_contrato is null
