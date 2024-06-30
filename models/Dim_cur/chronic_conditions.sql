 {%set tot_pat_count = "(select count(c.PATIENT) from {{ ref('fct_encounters')}} )" %}

with chronic_condition_q1 as (
select c.PATIENT,"START" start_dt, STOP end_dt,ENCOUNTER
  from {{ ref('fct_conditions') }} c
  where c.severity_type ='Chronic'  ),
  follow_up_q2 as (
    select e.PATIENT,e.ORGANIZATION, count(e.Id) cnt
      from {{ ref('fct_encounters') }} e
      join chronic_condition_q1 c
        on e.PATIENT = c.PATIENT
       --and e.id =c.ENCOUNTER
      where e."START" between c.start_dt and c.end_dt
       group by all
       HAVING COUNT(*) > 1   )  
select F.PATIENT,o.ADDRESS, (count(f.cnt)/(select count(ec.PATIENT)
       from  {{ ref('fct_encounters')}} ec ))*100 as percent_follow_up
  from {{ ref('fct_conditions') }} c
  join follow_up_q2 f
  on c.PATIENT = f.PATIENT
  join {{ ref('dim_organizations') }} o 
     on o.id = f.ORGANIZATION
   group by all
