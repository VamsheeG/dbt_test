with source_data as (
select Id, NAME, ADDRESS, CITY, STATE, ZIP, LAT, LON, PHONE, REVENUE, UTILIZATION
  from {{ source('raw','hmsorganizationdata')}}
)
select *
from source_data