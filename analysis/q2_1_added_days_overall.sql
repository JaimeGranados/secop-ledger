-- Title: Q2 · Added days, all contracts
-- Note: dias_adicionados is cumulative since signing (recorded by the source), current version.
-- Values over 3,650 days (10 years) are impossible and are left out of the median; they are counted.
select
    count(*) as contracts,
    count(*) filter (where dias_adicionados > 0) as with_added_days,
    round(100.0 * count(*) filter (where dias_adicionados > 0) / count(*), 1) as pct_with_added_days,
    median(dias_adicionados) filter (where dias_adicionados between 1 and 3650)
        as median_added_days,
    round(avg(dias_adicionados) filter (where dias_adicionados between 1 and 3650), 1)
        as avg_added_days,
    count(*) filter (where dias_adicionados > 3650) as excluded_over_10_years
from contract_history
where is_current
