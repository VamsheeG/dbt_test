with source_data as (
select *
  from {{ ref('chronic_conditions') }} 
)
select *
from source_data