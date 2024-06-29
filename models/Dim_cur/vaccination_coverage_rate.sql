{{ config(materialized='view')}}

WITH PAT_CNT AS
(SELECT COUNT(DISTINCT PATIENT) TOT_PATIENTS 
   FROM {{ ref('fct_encounters') }} E),
VAC_CNT AS (
select I.CODE,E.PROVIDER, COUNT(I.PATIENT) CNT
  from {{ ref('fct_immunizations') }} I
  JOIN {{ ref('dim_careplans') }} C
    ON I.PATIENT = C.PATIENT
   AND I.ENCOUNTER = C.ENCOUNTER
  JOIN {{ ref('fct_encounters') }} E
    ON E.PATIENT = C.PATIENT
   AND E.ID = C.ENCOUNTER 
   GROUP BY ALL
   )
SELECT TOT_PATIENTS, V.CODE,V.PROVIDER, V.CNT, (V.CNT/TOT_PATIENTS)*100 PERCENT
 FROM PAT_CNT P
 JOIN VAC_CNT V
 ORDER BY 3,2
