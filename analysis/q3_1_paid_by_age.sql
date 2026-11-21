-- Title: Q3 · Share of the contract value paid, by months since signing
-- Note: A cross-section on the latest collected day (each contract once, grouped by its age), not
-- yet one contract followed over time: that needs months of history. Only contracts with value > 0;
-- contracts with paid > value (a quality flag) are left out and counted. "Any payment" = the entity
-- reported a payment in SECOP; many entities report payments late or never.
with as_of as (
    select max(ingest_date) as day from fct_daily_quality where collected
),

contracts as (
    select
        date_diff('month', dc.fecha_de_firma, (select day from as_of)) as months,
        h.valor_pagado,
        h.valor_del_contrato
    from contract_history h
    join dim_contract dc on dc.id_contrato = h.id_contrato
    where h.is_current and h.valor_del_contrato > 0
)

select
    case
        when months < 1 then '0: less than 1 month'
        when months < 3 then '1: 1-2 months'
        when months < 6 then '2: 3-5 months'
        when months < 12 then '3: 6-11 months'
        else '4: 12+ months'
    end as age_since_signing,
    count(*) filter (where valor_pagado <= valor_del_contrato) as contracts,
    round(100.0 * count(*) filter (where valor_pagado > 0 and valor_pagado <= valor_del_contrato)
        / count(*) filter (where valor_pagado <= valor_del_contrato), 1) as pct_with_any_payment,
    round(100 * median(valor_pagado / valor_del_contrato)
        filter (where valor_pagado <= valor_del_contrato), 1) as median_pct_paid,
    count(*) filter (where valor_pagado > valor_del_contrato) as excluded_paid_over_value
from contracts
group by 1
order by 1
