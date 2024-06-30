{{ config(materialized='view')}}

with source_data as (
    select a.patient,o.city,
extract(year from a."START") yr,
extract(month from a."START") mon,
count(encounter) ALLERGY_COUNT
from {{ ref('allergy_management') }} a
join {{ ref('fct_encounters') }} e
  on a.encounter = e.id
join {{ ref('dim_organizations') }} o
  on o.id = e.organization
group by all )
select * from source_data