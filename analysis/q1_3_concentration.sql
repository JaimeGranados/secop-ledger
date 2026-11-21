-- Title: Q1 · How concentrated the modified contracts are in a few entities
-- Note: Share of all modified contracts that belong to the 10 entities with the most modified contracts.
with observed as (
    select distinct id_contrato
    from fct_contract_changes
    where column_name in ('valor_del_contrato', 'dias_adicionados', 'fecha_de_fin_del_contrato')
),

per_entity as (
    select dc.entity_key, count(*) as modified
    from dim_contract dc
    join contract_history h on h.id_contrato = dc.id_contrato and h.is_current
    left join observed o on o.id_contrato = dc.id_contrato
    where h.estado_contrato = 'Modificado' or h.dias_adicionados > 0 or o.id_contrato is not null
    group by dc.entity_key
),

ranked as (
    select *, row_number() over (order by modified desc) as rank from per_entity
)

select
    count(*) as entities_with_modified_contracts,
    sum(modified) as modified_contracts,
    sum(modified) filter (where rank <= 10) as modified_in_top_10,
    round(100.0 * sum(modified) filter (where rank <= 10) / sum(modified), 1) as pct_in_top_10
from ranked
