with source_data as (
select *
  from {{ ref('claims_denial_rate') }} 
)
select *
from source_data 