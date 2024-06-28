with allergies_q1 as (
select *
  from {{ ref('dim_allergies') }} 
  WHERE SEVERITY = 'SEVERE'
  )
  select *
  from allergies_q1