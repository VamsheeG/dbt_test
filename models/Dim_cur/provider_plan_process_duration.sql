{{ config(materialized='view')}}

with source_data as (
 SELECT E.PATIENT,E.PROVIDER,C.ID careplan_id,
        avg(datediff(minute,e."START",e.stop ))::number duration_minutes
   FROM {{ ref('fct_encounters') }} E
   JOIN {{ ref('dim_careplans') }} C
     ON E.PATIENT = C.PATIENT
    AND E.Id = C.ENCOUNTER
   group by all
  )
select patient,provider,careplan_id,duration_minutes
  from source_data
order by 1,2


