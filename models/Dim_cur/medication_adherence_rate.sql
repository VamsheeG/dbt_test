{{ config(materialized='view')}}

WITH source_data AS
(select count(ENCOUNTER_ID) tot_cnt,
  sum(cp_taken) tot_cp_taken,
  sum(mp_taken) tot_mp_taken,
  sum(cp_mp_taken_cnt) tot_cp_mp_taken_cnt,
  sum(cp_mp_not_taken_cnt) tot_cp_mp_not_taken_cnt,
  (TOT_CP_MP_TAKEN_CNT/TOT_CP_TAKEN)*100 CP_MP_PERCENT,
  (TOT_CP_MP_NOT_TAKEN_CNT/TOT_CP_TAKEN)*100 CP_MP_NOT_PERCENT
 FROM {{ ref('fct_medication_adherence_rate') }})
select * 
from source_data