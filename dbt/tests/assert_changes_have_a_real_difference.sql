-- A row in fct_contract_changes must be a real change: the old and new values differ.
select change_key, old_value, new_value
from {{ ref('fct_contract_changes') }}
where old_value is not distinct from new_value
