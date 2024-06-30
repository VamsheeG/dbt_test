{{ config (materialized='incremental',
           unique_key = ['UDI', 'PATIENT','ENCOUNTER'] 
            )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select  ZSTART AS "START",STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,UDI
  from {{ source('raw','hmsfctdevicesdata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select UDI TGT_UDI,PATIENT tgt_patient,ENCOUNTER tgt_encounter 
      from {{this}}
)
{% endif %}
select {{max_id}} + row_number() over (order by s."START" ) as id,
"START",STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,UDI
from source_data s

 {% if is_incremental() %}
left join tgt_data t
 on s.UDI = t.TGT_UDI
 and s.PATIENT = t.tgt_PATIENT
 and s.ENCOUNTER = t.tgt_ENCOUNTER
where 1 =1
  AND t.TGT_UDI is null
  and t.tgt_PATIENT is null 
  and t.tgt_ENCOUNTER is null 
{% else %}
 where 1 =1
{% endif %}