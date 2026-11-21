-- One row per changed VALUE: contract × day × column, with the old and the new value.
-- contract_history answers "how was the contract on day X"; this answers "what changed, by how much".
--
-- The 7 columns have different types (text, money, days, dates), so the SQL is generated with a
-- Jinja loop over var('tracked_columns'): one list, no copy-pasted CASE branches.
{% set tracked = var('tracked_columns') %}

with exploded as (
    select
        id_contrato,
        ingest_date,
        gap_days,
        dated_by_source,
        before_values,
        after_values,
        unnest(changed_columns) as column_name
    from {{ ref('stg_contract_changes') }}
    where change_type = 'changed'
)

select
    id_contrato || '@' || strftime(ingest_date, '%Y-%m-%d') || '@' || column_name as change_key,
    id_contrato,
    ingest_date as changed_on,  -- the day we SAW it (see gap_days)
    cast(strftime(ingest_date, '%Y%m%d') as integer) as changed_on_date_key,
    column_name,
    case column_name
        {%- for col, unit in tracked.items() %}
        when '{{ col }}' then '{{ unit }}'
        {%- endfor %}
    end as value_unit,
    case column_name
        {%- for col in tracked %}
        when '{{ col }}' then cast(before_values.{{ col }} as varchar)
        {%- endfor %}
    end as old_value,
    case column_name
        {%- for col in tracked %}
        when '{{ col }}' then cast(after_values.{{ col }} as varchar)
        {%- endfor %}
    end as new_value,
    -- How much it moved: COP for money, days for added days and for the end date. Null for status.
    case column_name
        {%- for col, unit in tracked.items() %}
        {%- if unit in ('COP', 'days') %}
        when '{{ col }}' then cast(after_values.{{ col }} - before_values.{{ col }} as decimal(38, 2))
        {%- elif unit == 'date' %}
        when '{{ col }}' then cast(datediff('day', before_values.{{ col }}, after_values.{{ col }}) as decimal(38, 2))
        {%- endif %}
        {%- endfor %}
    end as delta,
    gap_days,
    dated_by_source
from exploded
