{{ config(materialized='view')}}

with conditions_q1 as (
select dim_id,COUNT(*) cnt
  from {{ ref('fct_conditions') }} c
  group by 1 ),
  conditions_q2 as (
    SELECT ID,CODE,DESCRIPTION 
      from {{ ref('dim_conditions') }} e  )
select B.*,CNT,
 RANK() OVER ( ORDER BY CNT DESC,CODE) DIAGNOSED_RANK
FROM  conditions_q1 A
JOIN conditions_q2 B
 ON A.DIM_ID = B.ID
ORDER BY DIAGNOSED_RANK