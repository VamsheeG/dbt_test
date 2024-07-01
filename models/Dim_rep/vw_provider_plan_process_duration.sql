with source_data as (
select *
  from {{ ref('provider_plan_process_duration') }} 
)
select *
from source_data 