{{ config(materialized='view')}}

with source_data as (
 select C."START" out_come_dt,C.DIM_ID, i."DATE" immu_dt,i.PATIENT,i.CODE,i.description
  from {{ ref('fct_immunizations') }} I
  JOIN {{ ref('fct_conditions') }} C
    ON I.PATIENT = C.PATIENT
   AND I.ENCOUNTER = C.ENCOUNTER
   WHERE C."START" > I."DATE" 
  ),
  cond_data as 
  (select s.immu_dt,s.PATIENT,s.CODE,s.description,s.out_come_dt,c.code cond_code,c.description cond_description,c.SEVERITY_TYPE
     from {{ ref('dim_conditions') }} c
     join source_data S
       on s.dim_id = c.id )
select * 
 from cond_data