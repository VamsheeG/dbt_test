with encounter_patient as (
select e.PATIENT,e.Id, ZSTART,e.ENCOUNTERCLASS
  from {{ ref('dim_encounters') }} e
  join {{ ref('dim_patient') }} p
  on e.PATIENT=p.Id
)
select e.PATIENT,e.Id, e.ZSTART "START"
from encounter_patient e
where e.ENCOUNTERCLASS ='outpatient'