with source_data as (
select *
  from {{ ref('patient_visit_frequency') }} 
)
select *
from source_data 