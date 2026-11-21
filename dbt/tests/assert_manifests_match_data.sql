-- What each run SAYS it wrote (its _manifest.json) must be what is on disk.
-- A mismatch = a file was changed or deleted after the run, or a run wrote a wrong manifest.
with silver_manifest as (select * from {{ ref('stg_silver_runs') }}),
collector_manifest as (select * from {{ ref('stg_collector_runs') }}),
data as (select * from {{ ref('fct_daily_quality') }} where collected)

select
    d.ingest_date,
    cm.raw_rows as manifest_raw_rows, d.raw_rows as data_raw_rows,
    sm.snapshot_rows as manifest_snapshot_rows, d.snapshot_rows as data_snapshot_rows,
    sm.quarantine_rows as manifest_quarantine_rows, d.quarantine_rows as data_quarantine_rows
from data d
join collector_manifest cm on cm.ingest_date = d.ingest_date
left join silver_manifest sm on sm.ingest_date = d.ingest_date
where cm.raw_rows is distinct from d.raw_rows
   or (sm.ingest_date is not null and (
        sm.snapshot_rows is distinct from d.snapshot_rows
     or sm.quarantine_rows is distinct from d.quarantine_rows))
