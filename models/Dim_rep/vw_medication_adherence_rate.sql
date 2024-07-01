with source_data as (
select *
  from {{ ref('medication_adherence_rate') }} 
)
select *
from source_data 