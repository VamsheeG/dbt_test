{{ config (materialized='incremental',
            unique_key = ['PATIENT','ENCOUNTER','CODE'] 
            )}}

with source_data as (
select Id, ZSTART "START",STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION
  from {{ source('raw','hmscareplandata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select PATIENT,ENCOUNTER,CODE
      from {{this}}
)
{% endif %}
select s.Id,s."START",s.STOP,s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.REASONCODE,s.REASONDESCRIPTION
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
