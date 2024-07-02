with source_data as (
select *
  from {{ ref('patient_condition_outcome_rate') }} 
)
select *
from source_data  