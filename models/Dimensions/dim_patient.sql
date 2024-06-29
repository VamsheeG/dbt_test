with source_data as (
select Id ,BIRTHDATE ,DEATHDATE ,SSN ,PASSPORT ,PREFIX ,FIRST ,MIDDLE ,LAST ,SUFFIX ,MARITAL ,RACE ,ETHNICITY ,
GENDER ,ZIP ,HEALTHCARE_EXPENSES ,HEALTHCARE_COVERAGE ,INCOME
  from {{ source('raw','hmspatientdata')}}
)
select *
from source_data