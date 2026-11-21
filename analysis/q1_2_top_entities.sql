-- Title: Q1 · Entities that modify the largest share of their contracts (entities with 1,000+ contracts)
-- Note: Same definition of "modified" as Q1 · share. A high share is a fact, not evidence of wrongdoing:
-- extensions and value changes are normal in public contracting.
with observed as (
    select distinct id_contrato
    from fct_contract_changes
    where column_name in ('valor_del_contrato', 'dias_adicionados', 'fecha_de_fin_del_contrato')
),

contracts as (
    select
        dc.entity_key,
        h.estado_contrato = 'Modificado' or h.dias_adicionados > 0 or o.id_contrato is not null
            as modified
    from dim_contract dc
    join contract_history h on h.id_contrato = dc.id_contrato and h.is_current
    left join observed o on o.id_contrato = dc.id_contrato
)

select
    e.nombre_entidad as entity,
    e.sector,
    count(*) as contracts,
    count(*) filter (where c.modified) as modified,
    round(100.0 * count(*) filter (where c.modified) / count(*), 1) as pct_modified
from contracts c
join dim_entity e on e.entity_key = c.entity_key
group by e.entity_key, e.nombre_entidad, e.sector
having count(*) >= 1000
order by pct_modified desc, contracts desc
limit 10
