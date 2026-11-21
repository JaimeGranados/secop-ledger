-- Title: Q5 · Quality by day
-- Note: Missed days have no numbers (collected = false). Flags are counted on the snapshot of that day.
select
    ingest_date as day,
    collected,
    source_rows,
    duplicate_copies_removed as duplicates,
    quarantine_rows as quarantined,
    new_contracts as new,
    changed_contracts as changed,
    pct_changes_not_dated_by_source as pct_not_dated,
    flag_value_zero as value_zero,
    flag_paid_over_value as paid_over_value,
    flag_end_before_start as end_before_start
from fct_daily_quality
order by ingest_date
