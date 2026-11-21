-- One row per contract: its descriptive columns from the LAST snapshot where we saw it
-- (SCD Type 1 = only current values), plus the first and last day we saw it.
-- Reads every snapshot, so a contract that disappeared keeps the values it had on its last day.
--
-- Performance (measured on 6 snapshots = 11.9M rows, 2 GB / 2 threads):
--   arg_max of a struct with all the columns ........ 545 s (spilled to disk)
--   aggregate keys + join back (CTE used twice) ..... 480 s (DuckDB materialized the CTE = a copy
--                                                       of every row with its texts)
--   one window pass with QUALIFY (this) ............. a few seconds: the files are read once.
select
    id_contrato,
    min(ingest_date) over (partition by id_contrato) as first_seen_date,
    ingest_date as last_seen_date,
    referencia_del_contrato,
    proceso_de_compra,
    urlproceso,
    tipo_de_contrato,
    modalidad_de_contratacion,
    origen_de_los_recursos,
    fecha_de_firma,
    fecha_de_inicio_del_contrato,
    codigo_entidad,
    nit_entidad,
    nombre_entidad,
    departamento,
    ciudad,
    orden,
    sector,
    rama,
    entidad_centralizada,
    codigo_proveedor,
    tipodocproveedor,
    proveedor_adjudicado,
    es_pyme,
    es_grupo
from {{ source('silver', 'contracts_snapshot') }}
where filename not like '%.tmp%'
qualify ingest_date = max(ingest_date) over (partition by id_contrato)
