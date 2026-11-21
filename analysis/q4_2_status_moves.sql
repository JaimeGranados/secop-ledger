-- Title: Q4 · Status moves observed by the pipeline (top 10)
-- Note: Only moves seen since the first run. After a missed day, two moves can look like one.
select
    old_value as from_status,
    new_value as to_status,
    count(*) as moves,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_moves
from fct_contract_changes
where column_name = 'estado_contrato'
group by old_value, new_value
order by moves desc
limit 10
