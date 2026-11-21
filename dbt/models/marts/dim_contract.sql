-- One row per contract: what it is and who signed it (SCD Type 1, current values).
-- The tracked values that change over time live in contract_history.
select
    id_contrato,
    codigo_entidad as entity_key,
    codigo_proveedor as supplier_key,
    cast(strftime(fecha_de_firma, '%Y%m%d') as integer) as signed_date_key,
    referencia_del_contrato,
    proceso_de_compra,
    urlproceso,
    tipo_de_contrato,
    modalidad_de_contratacion,
    origen_de_los_recursos,
    fecha_de_firma,
    fecha_de_inicio_del_contrato,
    first_seen_date,
    last_seen_date,
    -- Active = present in the newest snapshot (it did not disappear from the source).
    last_seen_date = (select max(last_seen_date) from {{ ref('int_contracts_latest') }}) as is_active
from {{ ref('int_contracts_latest') }}
