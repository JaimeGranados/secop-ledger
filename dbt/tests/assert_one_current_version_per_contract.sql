-- A contract can never have two open versions at the same time.
select id_contrato, count(*) as current_versions
from {{ ref('contract_history') }}
where is_current
group by id_contrato
having count(*) > 1
