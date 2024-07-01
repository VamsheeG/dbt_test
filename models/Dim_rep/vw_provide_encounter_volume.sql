with source_data as (
select *
  from {{ ref('provider_encounter_volume') }} 
)
select *
from source_data 