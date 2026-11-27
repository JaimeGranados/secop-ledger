-- One row per contract: its descriptive columns as they are NOW (SCD Type 1 = only current
-- values), plus the first and last day we saw it.
--
-- Reads the attributes DELTA, not the daily snapshots: a full row is stored only on the days a
-- contract is new or changed, so the newest row per contract = its current values (the test
-- assert_attributes_match_latest_snapshot proves it with full_hash). This table no longer grows
-- with every collected day: about 2M rows on day 1 + ~5% per day, instead of 2M per day.
-- (Before: all snapshots, 214 s for dbt build on 11 days and growing.)
--
-- A contract that disappeared keeps the values of its last attribute row.
-- One window pass with QUALIFY (a CTE used twice was copied by DuckDB: 480 s).
with attributes as (
    select
        *,
        min(ingest_date) over (partition by id_contrato) as first_seen_date
    from {{ source('silver', 'contracts_attributes') }}
    where filename not like '%.tmp%'
    qualify ingest_date = max(ingest_date) over (partition by id_contrato)
),

-- The last day we saw it: the newest collected day, unless its newest event is "disappeared"
-- (then the day before it disappeared = the previous snapshot of that event).
last_events as (
    select id_contrato, change_type, prev_ingest_date
    from {{ ref('stg_contract_changes') }}
    qualify ingest_date = max(ingest_date) over (partition by id_contrato)
)

select
    a.id_contrato,
    a.first_seen_date,
    case
        when e.change_type = 'disappeared' then e.prev_ingest_date
        else (select max(ingest_date) from {{ ref('stg_contract_changes') }})
    end as last_seen_date,
    a.referencia_del_contrato,
    a.proceso_de_compra,
    a.urlproceso,
    a.tipo_de_contrato,
    a.modalidad_de_contratacion,
    a.origen_de_los_recursos,
    a.fecha_de_firma,
    a.fecha_de_inicio_del_contrato,
    a.codigo_entidad,
    a.nit_entidad,
    a.nombre_entidad,
    a.departamento,
    a.ciudad,
    a.orden,
    a.sector,
    a.rama,
    a.entidad_centralizada,
    a.codigo_proveedor,
    a.tipodocproveedor,
    a.proveedor_adjudicado,
    a.es_pyme,
    a.es_grupo
from attributes a
left join last_events e on e.id_contrato = a.id_contrato
