-- Every collected day must close all its accounts:
--   raw      = source count(*)                                  (within the collector's 0.5%)
--   raw      = snapshot + quarantine + duplicate copies removed (silver)
--   snapshot = previous snapshot + new - disappeared            (changes; day 1 = baseline)
with days as (
    select
        *,
        lag(snapshot_rows) over (order by ingest_date) as prev_snapshot_rows
    from {{ ref('fct_daily_quality') }}
    where collected and snapshot_rows is not null
)

select ingest_date, source_rows, raw_rows, snapshot_rows, prev_snapshot_rows,
       baseline_contracts, new_contracts, disappeared_contracts
from days
where abs(raw_minus_source) > 0.005 * source_rows
   or not silver_reconciled
   or snapshot_rows <> coalesce(prev_snapshot_rows, 0)
        + coalesce(baseline_contracts, 0) + coalesce(new_contracts, 0)
        - coalesce(disappeared_contracts, 0)
