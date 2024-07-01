with source_data as (
select *
  from {{ ref('medication_condition') }} 
)
select *
from source_data 