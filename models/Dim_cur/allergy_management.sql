{{ config (materialized='incremental',
            unique_key = ['PATIENT','ENCOUNTER','ALLERGY_ID','REACTION'] 
            )}}
with allergies_q1 as (
select *
  from {{ ref('fct_allergies') }} 
    WHERE SEVERITY = 'SEVERE'
  )
 {% if is_incremental() %}
,
tgt_data as (
    select  PATIENT TPATIENT,ENCOUNTER TENCOUNTER,ALLERGY_ID TALLERGY_ID,REACTION TREACTION
      from {{this}}
)
{% endif %}

  select *
  from allergies_q1 s

  {% if is_incremental() %}
left join tgt_data t
  ON S.PATIENT = T.TPATIENT
 AND S.ENCOUNTER = T.TENCOUNTER
 AND S.ALLERGY_ID = T.TALLERGY_ID 
 AND S.REACTION = T.TREACTION
WHERE 1 =1
  AND T.TPATIENT IS NULL 
  AND T.TENCOUNTER IS NULL 
  AND T.TALLERGY_ID IS NULL 
  AND T.TREACTION IS NULL
{% else %}
 where 1 =1
{% endif %}