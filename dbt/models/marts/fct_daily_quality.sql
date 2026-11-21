-- One row per CALENDAR day from the first to the last collected day, also the days we missed
-- (collected = false). Answers Q5 ("how dirty is the source every day?") and the pipeline numbers.
--
-- Rule: counts come from the DATA. The manifests give only what exists nowhere else: the source
-- count(*) at download time, the run time, and how many duplicate copies silver removed.
-- assert_manifests_match_data checks that what each run SAYS it wrote is what is on disk.
with collector as (
    select * from {{ ref('stg_collector_runs') }}
),

bounds as (
    select min(ingest_date) as first_day, max(ingest_date) as last_day from collector
),

calendar as (
    select d.full_date as day, d.date_key
    from {{ ref('dim_date') }} d, bounds b
    where d.full_date between b.first_day and b.last_day
),

raw_counts as (
    select
        cast(regexp_extract(filename, 'ingest_date=(\d{4}-\d{2}-\d{2})', 1) as date) as ingest_date,
        count(*) as raw_rows
    from {{ source('raw', 'contracts') }}
    group by 1
),

snapshot_counts as (
    select
        ingest_date,
        count(*) as snapshot_rows,
        count(*) filter (where flag_value_zero) as flag_value_zero,
        count(*) filter (where flag_implausible_value) as flag_implausible_value,
        count(*) filter (where flag_implausible_date) as flag_implausible_date,
        count(*) filter (where flag_paid_over_value) as flag_paid_over_value,
        count(*) filter (where flag_invoiced_over_value) as flag_invoiced_over_value,
        count(*) filter (where flag_negative_pending) as flag_negative_pending,
        count(*) filter (where flag_end_before_start) as flag_end_before_start
    from {{ source('silver', 'contracts_snapshot') }}
    group by 1
),

quarantine_counts as (
    select ingest_date, count(*) as quarantine_rows
    from {{ source('silver', 'contracts_quarantine') }}
    group by 1
),

change_counts as (
    select
        ingest_date,
        count(*) filter (where change_type = 'baseline') as baseline_contracts,
        count(*) filter (where change_type = 'new') as new_contracts,
        count(*) filter (where change_type = 'changed') as changed_contracts,
        count(*) filter (where change_type = 'disappeared') as disappeared_contracts,
        count(*) filter (where change_type = 'changed' and not dated_by_source)
            as changed_not_dated_by_source
    from {{ ref('stg_contract_changes') }}
    group by 1
),

collected_days as (
    select
        ingest_date,
        datediff('day', lag(ingest_date) over (order by ingest_date), ingest_date) - 1
            as missed_days_before
    from collector
)

select
    c.day as ingest_date,
    c.date_key,
    col.ingest_date is not null as collected,
    cd.missed_days_before,

    -- Collector
    col.source_rows,
    r.raw_rows,
    r.raw_rows - col.source_rows as raw_minus_source,
    round(col.download_seconds / 60, 1) as download_minutes,

    -- Silver
    s.snapshot_rows,
    coalesce(q.quarantine_rows, 0) as quarantine_rows,
    sr.duplicate_copies_removed,
    r.raw_rows = s.snapshot_rows + coalesce(q.quarantine_rows, 0) + sr.duplicate_copies_removed
        as silver_reconciled,
    s.flag_value_zero,
    s.flag_implausible_value,
    s.flag_implausible_date,
    s.flag_paid_over_value,
    s.flag_invoiced_over_value,
    s.flag_negative_pending,
    s.flag_end_before_start,

    -- Changes
    ch.baseline_contracts,
    ch.new_contracts,
    ch.changed_contracts,
    ch.disappeared_contracts,
    ch.changed_not_dated_by_source,
    round(100.0 * ch.changed_not_dated_by_source / nullif(ch.changed_contracts, 0), 1)
        as pct_changes_not_dated_by_source
from calendar c
left join collector col on col.ingest_date = c.day
left join collected_days cd on cd.ingest_date = c.day
left join raw_counts r on r.ingest_date = c.day
left join snapshot_counts s on s.ingest_date = c.day
left join quarantine_counts q on q.ingest_date = c.day
left join {{ ref('stg_silver_runs') }} sr on sr.ingest_date = c.day
left join change_counts ch on ch.ingest_date = c.day
