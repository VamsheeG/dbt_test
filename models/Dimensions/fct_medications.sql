{{ config (materialized='incremental',
           unique_key = ['"START"','STOP', 'PATIENT','PAYER','ENCOUNTER','CODE'] 
            )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select ZSTART AS "START",STOP,PATIENT,PAYER,ENCOUNTER,CODE,DESCRIPTION,
BASE_COST,PAYER_COVERAGE,DISPENSES,TOTALCOST,REASONCODE,REASONDESCRIPTION
  from {{ source('raw','hmsfctmedicationdata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select "START" tgt_start,STOP tgt_stop,PATIENT tgt_patient,PAYER tgt_payer,ENCOUNTER tgt_encounter,CODE tgt_code 
      from {{this}}
)
{% endif %}
select {{max_id}} + row_number() over (order by s."START" ) as id,
s."START", s.STOP,s.PATIENT,s.PAYER,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.BASE_COST,s.PAYER_COVERAGE,s.DISPENSES,
s.TOTALCOST,s.REASONCODE,s.REASONDESCRIPTION
from source_data s

 {% if is_incremental() %}
left join tgt_data t
 on s.PATIENT = t.tgt_PATIENT
 and s.PAYER = t.tgt_PAYER
 and s.ENCOUNTER = t.tgt_ENCOUNTER
 and s.CODE = t.tgt_CODE
 and s."START" = t.tgt_start
 and s.STOP = t.tgt_STOP
where 1 =1
  and t.tgt_PATIENT is null 
  and t.tgt_PAYER is null 
  and t.tgt_ENCOUNTER is null 
  and t.tgt_CODE is null
  and t.tgt_START is null
  and t.tgt_STOP is null
{% else %}
 where 1 =1
{% endif %}