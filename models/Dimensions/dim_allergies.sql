{{ config (materialized='incremental',
            unique_key = ['CODE','DESCRIPTION','TYPE','CATEGORY'] 
            )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) max_id from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select distinct CODE,DESCRIPTION,TYPE,CATEGORY
  from {{ source('raw','hmsallergydata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select CODE,DESCRIPTION,TYPE,CATEGORY
      from {{this}}
)
{% endif %}
select {{max_id}} + row_number() over (order by s.CODE ) as id, s.CODE,s.DESCRIPTION,s.TYPE,s.CATEGORY
from source_data s

{% if is_incremental() %}
left join tgt_data t
  on s.CODE = t.CODE
 and s.DESCRIPTION = t.DESCRIPTION
 and s.TYPE = t.type 
 and s.CATEGORY = t.CATEGORY
where 1 =1
  and t.CODE is null 
  and t.DESCRIPTION is null 
  and t.TYPE is null 
  and t.CATEGORY is null
{% else %}
 where 1 =1
{% endif %}
