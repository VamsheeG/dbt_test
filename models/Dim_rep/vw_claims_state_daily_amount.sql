with source_data as (
select *
  from {{ ref('claims_state_daily_amount') }} 
)
select *
from source_data 