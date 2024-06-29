{{ config (materialized='incremental',
           unique_key = ['PATIENT','ENCOUNTER','SYSTEM'] 
            )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select ZSTART AS "START",STOP,PATIENT,ENCOUNTER,CODE,SYSTEM,DESCRIPTION,TYPE,CATEGORY,REACTION1 REACTION,DESCRIPTION1 RDESCRIPTION,SEVERITY1 SEVERITY
  from {{ source('raw','hmsfctallergydata')}}
union
select ZSTART AS "START",STOP,PATIENT,ENCOUNTER,CODE,SYSTEM,DESCRIPTION,TYPE,CATEGORY,REACTION2 REACTION,DESCRIPTION2 RDESCRIPTION,SEVERITY2 SEVERITY
from {{ source('raw','hmsfctallergydata')}}  
),
src_dim as(
    select id,CODE,DESCRIPTION,TYPE,CATEGORY
      from {{ ref('dim_allergies') }}
)
{% if is_incremental() %}
,
tgt_data as (
    select PATIENT,ENCOUNTER,SYSTEM
      from {{this}}
)
{% endif %}
select {{max_id}} + row_number() over (order by "START" ) as id,
"START", s.STOP,s.PATIENT,s.ENCOUNTER,s.system,sd.id allergy_id,s.REACTION,s.RDESCRIPTION,s.SEVERITY
from source_data s
join src_dim sd 
  on s.CODE = sd.CODE
 and s.DESCRIPTION = sd.DESCRIPTION
 and s.TYPE = sd.TYPE
 and s.CATEGORY = sd.CATEGORY

 {% if is_incremental() %}
left join tgt_data t
 on s.PATIENT = t.PATIENT
 and s.ENCOUNTER = t.ENCOUNTER
 and s.SYSTEM = t.SYSTEM
where 1 =1
  and t.PATIENT is null 
  and t.ENCOUNTER is null 
  and t.SYSTEM is null
{% else %}
 where 1 =1
{% endif %}