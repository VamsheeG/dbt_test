{{ config(materialized='view')}}

with source_data as (
select d.patient, d.code, d."START" start_dt, d.stop end_dt,
c."START" pln_dt, datediff(day,start_dt,end_dt) as usage_dur
from {{ ref('fct_devices') }} d
join {{ ref('dim_careplans') }} c
  on d.encounter = c.encounter
 and d.patient = c.patient
where d."START" >= c."START")
select patient,code,pln_dt,count(code) as use_freq,
       sum(usage_dur) tot_dur_used,
       avg(usage_dur) avg_dur_used
from source_data
group by all
order by 1,2