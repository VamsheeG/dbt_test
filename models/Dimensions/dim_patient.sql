with source_data as (
select *
  from {{ source('raw','hmsstagedata')}}
)
select *
from source_data