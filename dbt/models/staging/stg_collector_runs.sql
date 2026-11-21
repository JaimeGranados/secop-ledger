-- One row per day the collector PUBLISHED (a failed day has no manifest).
select
    ingest_date,
    source_count as source_rows,  -- count(*) at the source when the run started
    "rows" as raw_rows,           -- rows written to raw
    seconds as download_seconds,
    started_at,
    finished_at
from {{ source('raw', 'collector_manifests') }}
where filename not like '%.tmp%'
