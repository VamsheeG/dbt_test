with source_data as (
select *
  from {{ ref('average_claim_processing_time') }} 
)
select *
from source_data 