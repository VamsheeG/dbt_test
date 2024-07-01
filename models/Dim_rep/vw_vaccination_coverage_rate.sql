with source_data as (
select *
  from {{ ref('vaccination_coverage_rate') }} 
)
select *
from source_data 