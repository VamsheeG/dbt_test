with source_data as (
select *
  from {{ ref('device_utilization_rate') }} 
)
select *
from source_data 