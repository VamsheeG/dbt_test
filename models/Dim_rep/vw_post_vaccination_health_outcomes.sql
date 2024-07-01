with source_data as (
select *
  from {{ ref('post_vaccination_health_outcomes') }} 
)
select *
from source_data  