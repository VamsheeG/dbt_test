with provider_data as (
select PROVIDER,
extract(year from "START") AS yr,
extract(month from "START") AS mon,
count(distinct PATIENT) Patient_cnt
  from {{ ref('fct_encounters') }} 
  group by PROVIDER,
extract(year from "START"),
extract(month from "START")
)
select * 
from provider_data