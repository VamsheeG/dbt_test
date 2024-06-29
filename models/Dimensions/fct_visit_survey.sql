{{ config (materialized='incremental',
  unique_key = ['PATIENT_ID','SURVEY_DATE']  )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select patient_id,survey_date,rating,comment
  from {{ source('raw','surveydata')}}
)

{% if is_incremental() %}
,
tgt_data as (
    select S.patient_id,S.survey_date
      from {{this}} S
)
{% endif %}

select  {{max_id}} + row_number() over (order by S.survey_date ) as id,
S.patient_id,S.survey_date,S.rating,S.comment
from source_data s

{% if is_incremental() %}
left join tgt_data t
  on s.patient_id = t.patient_id
 and s.survey_date = t.survey_date
where 1 =1
  and t.patient_id is null 
  and t.survey_date is null 
{% else %}
 where 1 =1
{% endif %}