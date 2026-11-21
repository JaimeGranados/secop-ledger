-- Title: Q2 · Added days by sector (the 10 sectors with the most contracts)
-- Note: Median over contracts that got added days (1 to 3,650).
select
    e.sector,
    count(*) as contracts,
    round(100.0 * count(*) filter (where h.dias_adicionados > 0) / count(*), 1) as pct_with_added_days,
    median(h.dias_adicionados) filter (where h.dias_adicionados between 1 and 3650)
        as median_added_days
from contract_history h
join dim_contract dc on dc.id_contrato = h.id_contrato
join dim_entity e on e.entity_key = dc.entity_key
where h.is_current
group by e.sector
order by contracts desc
limit 10
