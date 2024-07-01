with source_data as (
select *
  from {{ ref('top_diagnosed_conditions') }} 
)
select *
from source_data  