{{ config (materialized='incremental',
  unique_key = ['PATIENT','ENCOUNTER','DIM_ID']  )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select ZSTART as "START",STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION
 from {{ source('raw','conditionsfctdata')}}
),
src_dim as(
    select id dim_id,CODE,DESCRIPTION,SEVERITY_TYPE
      from {{ ref('dim_conditions') }}
)
{% if is_incremental() %}
,
tgt_data as (
    select s.PATIENT,s.ENCOUNTER,S.dim_id
      from {{this}} S
)
{% endif %}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}
select {{max_id}} + row_number() over (order by s."START" ) as id,
"START", s.STOP,s.PATIENT,s.ENCOUNTER,sd.dim_id,sd.SEVERITY_TYPE
from source_data s
join src_dim sd 
  on s.CODE = sd.CODE
 and s.DESCRIPTION = sd.DESCRIPTION

 {% if is_incremental() %}
left join tgt_data t
  on s.PATIENT = t.PATIENT
 and s.ENCOUNTER = t.ENCOUNTER
where 1 =1
  and t.PATIENT is null 
  and t.ENCOUNTER is null 
{% else %}
 where 1 =1
{% endif %}