-- One row per public entity (SCD Type 1). The source repeats the entity on every contract, and a
-- few entities have different names on different contracts (9 on 2026-11-21): we keep the values
-- from the most recently signed contract (ties broken by id_contrato, so the result is stable).
select
    codigo_entidad as entity_key,
    arg_max(nit_entidad, (fecha_de_firma, id_contrato)) as nit_entidad,
    arg_max(nombre_entidad, (fecha_de_firma, id_contrato)) as nombre_entidad,
    arg_max(departamento, (fecha_de_firma, id_contrato)) as departamento,
    arg_max(ciudad, (fecha_de_firma, id_contrato)) as ciudad,
    arg_max(orden, (fecha_de_firma, id_contrato)) as orden,
    arg_max(sector, (fecha_de_firma, id_contrato)) as sector,
    arg_max(rama, (fecha_de_firma, id_contrato)) as rama,
    arg_max(entidad_centralizada, (fecha_de_firma, id_contrato)) as entidad_centralizada,
    count(*) as contracts
from {{ ref('int_contracts_latest') }}
group by codigo_entidad
