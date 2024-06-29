with chronic_condition_q1 as (
select c.PATIENT,min("START") start_dt
  from {{ ref('fct_conditions') }} c
  where c.severity_type ='Chronic'
  group by 1
  ),
  follow_up_q2 as (
    select e.PATIENT,count(Id) cnt
      from {{ ref('fct_encounters') }} e
      join chronic_condition_q1 c
        on e.PATIENT = c.PATIENT
       and e."START" > c.start_dt
       group by 1 
  )
select (count(f.PATIENT)/count(distinct c.PATIENT)) as percent_follow_up
  from {{ ref('fct_conditions') }} c
  left join follow_up_q2 f
  on c.PATIENT = f.PATIENT


