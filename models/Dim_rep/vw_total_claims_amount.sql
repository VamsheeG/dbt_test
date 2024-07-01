with source_data as (
select *
  from {{ ref('total_claims_amount') }} 
)
select *
from source_data