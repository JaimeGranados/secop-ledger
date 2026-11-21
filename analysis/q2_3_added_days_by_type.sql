-- Title: Q2 · Added days by contract type (types with 1,000+ contracts)
-- Note: Median over contracts that got added days (1 to 3,650).
select
    dc.tipo_de_contrato as contract_type,
    count(*) as contracts,
    round(100.0 * count(*) filter (where h.dias_adicionados > 0) / count(*), 1) as pct_with_added_days,
    median(h.dias_adicionados) filter (where h.dias_adicionados between 1 and 3650)
        as median_added_days
from contract_history h
join dim_contract dc on dc.id_contrato = h.id_contrato
where h.is_current
group by dc.tipo_de_contrato
having count(*) >= 1000
order by pct_with_added_days desc
