{{ config (materialized='incremental',
            unique_key = ['ID'] 
            )}}
with source_data as (
select Id, NAME, ADDRESS, CITY, STATE, ZIP, LAT, LON, PHONE, REVENUE, UTILIZATION
  from {{ source('raw','hmsorganizationdata')}}
)
{% if is_incremental() %}
,
tgt_data as (
    select  Id tid
      from {{this}}
)
{% endif %}

select *
from source_data S

  {% if is_incremental() %}
left join tgt_data t
  ON S.ID = T.tid
WHERE 1 =1
  AND T.TID IS NULL 
{% else %}
 where 1 =1
{% endif %}