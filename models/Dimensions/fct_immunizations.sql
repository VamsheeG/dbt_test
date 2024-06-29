{{ config (materialized='incremental',
unique_key = ['PATIENT','ENCOUNTER','CODE'] )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select zdate "DATE",PATIENT,ENCOUNTER,CODE,DESCRIPTION,BASE_COST
  from {{ source('raw','immunizationsdata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select PATIENT,ENCOUNTER,CODE
      from {{this}}
)
{% endif %}

select {{max_id}} + row_number() over (order by S.CODE ) as id,
 "DATE",s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.BASE_COST
from source_data s

{% if is_incremental() %}
left join tgt_data t
  on s.PATIENT = t.PATIENT
 and s.ENCOUNTER = t.ENCOUNTER
 and s.CODE = t.CODE
 where 1 =1
  and t.PATIENT is null 
  and t.ENCOUNTER is null 
  and t.CODE is null 
{% else %}
 where 1 =1
{% endif %}