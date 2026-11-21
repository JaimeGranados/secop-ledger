-- Title: Q5 · How dirty the source is (summary of the collected days)
-- Note: Every number comes from fct_daily_quality, where every collected day is reconciled
-- (raw = source; raw = snapshot + quarantine + duplicates removed) by dbt tests.
-- Duplicates are counted per day: the same 111 copies came on 11-10 and 11-11, then the source
-- stopped sending them.
select
    count(*) as calendar_days,
    count(*) filter (where collected) as days_collected,
    count(*) filter (where not collected) as days_missed,
    sum(duplicate_copies_removed) as duplicate_copies_removed,
    sum(quarantine_rows) as rows_quarantined,
    count(*) filter (where collected and not silver_reconciled) as days_not_reconciled,
    sum(changed_contracts) as changed_contracts,
    sum(changed_not_dated_by_source) as changed_not_dated_by_source,
    round(100.0 * sum(changed_not_dated_by_source) / sum(changed_contracts), 1)
        as pct_changes_not_dated_by_source
from fct_daily_quality
