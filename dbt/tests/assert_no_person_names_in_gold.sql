-- Personal data rule: gold shows a supplier name only for companies (NIT).
-- Any row here = a person's name leaked into gold.
select supplier_key, document_type, supplier_kind
from {{ ref('dim_supplier') }}
where supplier_name is not null
  and (supplier_kind <> 'company' or document_type is distinct from 'NIT')
