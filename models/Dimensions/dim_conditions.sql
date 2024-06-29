{{ config (materialized='incremental',
unique_key = ['CODE','DESCRIPTION','SEVERITY_TYPE'] )}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select distinct CODE,DESCRIPTION 
  from {{ source('raw','conditionsdata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select CODE,DESCRIPTION,SEVERITY_TYPE
      from {{this}}
)
{% endif %}

select {{max_id}} + row_number() over (order by S.CODE ) as id,s.code,s.description,
 split_part(s.description,' ',1) as severity_type
from source_data s

{% if is_incremental() %}
left join tgt_data t
  on s.CODE = t.CODE
 and s.DESCRIPTION = t.DESCRIPTION
 and split_part(s.description,' ',1)= t.SEVERITY_TYPE 
 where 1 =1
  and t.CODE is null 
  and t.DESCRIPTION is null 
  and t.SEVERITY_TYPE is null 
{% else %}
 where 1 =1
{% endif %}