with encounter_patient as (
select e.PATIENT,p."FIRST",	p.MIDDLE,p."LAST",
 e.ORGANIZATION, e.ENCOUNTERCLASS,
 extract(year from e."START") AS yr,
 extract(month from e."START") AS mon,
 count(e.Id) no_of_visits
  from {{ ref('fct_encounters') }} e
  join {{ ref('dim_patient') }} p
  on e.PATIENT=p.Id
  group by all
)
select e.PATIENT,e."FIRST", e.MIDDLE,e."LAST",e.ORGANIZATION, e.ENCOUNTERCLASS,e.yr,e.mon,e.no_of_visits
from encounter_patient e
where e.ENCOUNTERCLASS= 'outpatient'