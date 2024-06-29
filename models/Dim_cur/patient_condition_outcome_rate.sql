{{ config(materialized='view')}}

with source_data as (
select s.patient_id,case when rating < 3 then 'deteriorated'
                    else 'Improved' end out_come
  from {{ ref('fct_encounters') }} e
  join {{ ref('fct_visit_survey') }} s
    on e.patient = s.patient_id
   AND e."START" = S.SURVEY_DATE )
select a.patient_id,a.out_come
FROM  source_data A
order by a.patient_id