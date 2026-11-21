-- One row per day the silver build published: what the run SAYS it did.
-- fct_daily_quality counts the same things from the data, and a test compares both.
select
    ingest_date,
    raw_rows,
    snapshot_rows,
    quarantine_rows,
    duplicate_copies_removed,
    conflicting_duplicate_ids
from {{ source('silver', 'snapshot_manifests') }}
where filename not like '%.tmp%'
