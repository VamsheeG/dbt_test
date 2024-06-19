with source_data as (
select *
  from {{ source('raw','deptstagedata')}}
)
select *
from source_data