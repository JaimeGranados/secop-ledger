-- Title: Q1 · Share of contracts modified after signing, by signing year
-- Note: "Modified" = the terms changed after signing: current status "Modificado", or added days
-- (both recorded by the source since signing), or a change of contract value, added days or end
-- date that the pipeline observed itself. Payments moving is not a modification.
with observed as (
    select distinct id_contrato
    from fct_contract_changes
    where column_name in ('valor_del_contrato', 'dias_adicionados', 'fecha_de_fin_del_contrato')
),

contracts as (
    select
        cast(year(dc.fecha_de_firma) as varchar) as signing_year,
        h.estado_contrato = 'Modificado' or h.dias_adicionados > 0 or o.id_contrato is not null
            as modified
    from dim_contract dc
    join contract_history h on h.id_contrato = dc.id_contrato and h.is_current
    left join observed o on o.id_contrato = dc.id_contrato
)

select
    signing_year,
    count(*) as contracts,
    count(*) filter (where modified) as modified,
    round(100.0 * count(*) filter (where modified) / count(*), 1) as pct_modified
from contracts
group by signing_year
order by signing_year
