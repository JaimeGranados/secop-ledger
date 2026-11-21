-- One row per supplier (SCD Type 1).
--
-- Personal data: about 91% of suppliers are people (Cédula). Their name is personal data, so gold
-- only shows the name of COMPANIES (NIT). People get supplier_kind = 'person' and no name.
-- The names stay only in the local silver layer. A test checks this rule.
with suppliers as (
    select
        codigo_proveedor,
        arg_max(tipodocproveedor, (fecha_de_firma, id_contrato)) as tipodocproveedor,
        arg_max(proveedor_adjudicado, (fecha_de_firma, id_contrato)) as proveedor_adjudicado,
        arg_max(es_pyme, (fecha_de_firma, id_contrato)) as es_pyme,
        arg_max(es_grupo, (fecha_de_firma, id_contrato)) as es_grupo,
        count(*) as contracts
    from {{ ref('int_contracts_latest') }}
    group by codigo_proveedor
)

select
    codigo_proveedor as supplier_key,
    tipodocproveedor as document_type,
    case
        when tipodocproveedor = 'NIT' then 'company'
        when tipodocproveedor is null or tipodocproveedor = 'Otro' then 'unknown'
        else 'person'
    end as supplier_kind,
    case when tipodocproveedor = 'NIT' then proveedor_adjudicado end as supplier_name,
    es_pyme = 'Si' as is_sme,
    es_grupo = 'Si' as is_group,  -- a consortium or temporary union
    contracts
from suppliers
