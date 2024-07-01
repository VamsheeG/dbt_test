with source_data as (
select PATIENT,CITY,YR,MON,ALLERGY_COUNT
  from {{ ref('allergy_management_vw') }} 
)
select PATIENT,CITY,YR,MON,ALLERGY_COUNT
from source_data 