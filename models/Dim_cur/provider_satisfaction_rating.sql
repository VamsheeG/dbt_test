with provider_satisy as (
select E.PROVIDER,
extract(year from SURVEY_DATE) AS yr,
extract(month from SURVEY_DATE) AS mon,
 AVG(S.RATING) AVG_RATING
  from {{ ref('fct_encounters') }} e 
  join {{ ref('fct_visit_survey') }} s
    on e.patient = s.patient_id
   AND e."START" = S.SURVEY_DATE
  group by PROVIDER,
  extract(year from SURVEY_DATE),
  extract(month from SURVEY_DATE)
)
select * 
from provider_satisy