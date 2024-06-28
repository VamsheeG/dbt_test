{{ config (materialized='incremental')}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select ZSTART AS "START",STOP,PATIENT,ENCOUNTER,CODE,SYSTEM,DESCRIPTION,TYPE,CATEGORY,REACTION1 REACTION,DESCRIPTION1 RDESCRIPTION,SEVERITY1 SEVERITY
  from {{ source('raw','hmsallergydata')}}
union all
select ZSTART AS "START",STOP,PATIENT,ENCOUNTER,CODE,SYSTEM,DESCRIPTION,TYPE,CATEGORY,REACTION2 REACTION,DESCRIPTION2 RDESCRIPTION,SEVERITY2 SEVERITY
from {{ source('raw','hmsallergydata')}}  
)
select {{max_id}} + row_number() over (order by "START" ) as id, *, YEAR("START") YR, MONTH("START") MON
from source_data
where 1 =1
{% if is_incremental() %}
   and "START" > (select max("START") from {{this}} )
{% endif %}