{{ config(materialized='view')}}

with conditions_data as (
select * 
  from {{ ref('fct_conditions') }} f
  join {{ ref('dim_conditions') }} d
    on f.dim_id=d.id
),
source_data as (
select m.code medication_code,c.code condition_code,
  avg(CASE WHEN C."START" >= CP."START" AND C.STOP <= CP.STOP THEN 1 ELSE 0 END)::number AS medication_effective
  from {{ ref('fct_medications') }} m
  join {{ ref('dim_careplans') }} cp
    on m.encounter = cp.encounter
   and m.patient = cp.patient
  join conditions_data c
    on m.encounter = c.encounter
   and m.patient = c.patient 
  where m."START" <= C.STOP
  group by all
) 
select * 
from source_data
order by 1,2