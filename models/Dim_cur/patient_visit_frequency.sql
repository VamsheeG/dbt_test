with encounter_patient as (
select e.PATIENT,e.Id, "START",e.ENCOUNTERCLASS
  from {{ ref('fct_encounters') }} e
  join {{ ref('dim_patient') }} p
  on e.PATIENT=p.Id
)
select e.PATIENT,e.Id, e."START"
from encounter_patient e
where e.ENCOUNTERCLASS ='outpatient'