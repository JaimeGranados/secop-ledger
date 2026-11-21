-- Title: Q4 · Where contracts are now (current status)
-- Note: "How long contracts stay in each status" needs a status to start AND end inside our
-- history; with a few days of history that would only measure very short stays. Measured later.
select
    estado_contrato as status,
    count(*) as contracts,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from contract_history
where is_current
group by estado_contrato
order by contracts desc
