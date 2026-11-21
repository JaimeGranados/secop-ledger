-- One row per calendar day. Built with DuckDB's range(), no package needed.
-- The range is set in dbt_project.yml (vars) and covers every date the facts can point to.
with days as (
    select cast(d as date) as full_date
    from range(
        date '{{ var("date_start") }}',
        date '{{ var("date_end") }}' + interval 1 day,
        interval 1 day
    ) as t(d)
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    strftime(full_date, '%B') as month_name,
    day(full_date) as day_of_month,
    isodow(full_date) as iso_day_of_week,  -- 1 = Monday, 7 = Sunday
    strftime(full_date, '%A') as day_name,
    weekofyear(full_date) as iso_week,
    isodow(full_date) in (6, 7) as is_weekend
from days
