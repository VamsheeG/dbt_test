with source_data as (
select *
  from {{ source('raw','encountersdata')}}
)
select *
from source_data