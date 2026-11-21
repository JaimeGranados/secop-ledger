-- Each version must close on or before the day the next one opens.
-- (Before = the contract disappeared for a while and came back: a hole, not an overlap.)
with ordered as (
    select
        id_contrato,
        version_key,
        valid_to,
        lead(valid_from) over (partition by id_contrato order by valid_from) as next_valid_from
    from {{ ref('contract_history') }}
)

select *
from ordered
where next_valid_from is not null
  and (valid_to is null or valid_to > next_valid_from)
