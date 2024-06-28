{{ config (materialized='incremental')}}

{% if is_incremental() %}
 {%set max_id = "(select max(id) from " ~ this ~ ")" %}
{% else %}
 {%set max_id =0 %}
{% endif %}

with source_data as (
select *
  from {{ source('raw','conditionsdata')}}
)
select {{max_id}} + row_number() over (order by ZSTART ) as id, *,split_part(description,' ',1) as severity_type
from source_data
where 1 =1
{% if is_incremental() %}
   and zstart > (select max(zstart) from {{this}} )
{% endif %}