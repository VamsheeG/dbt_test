with source_data as (
SELECT  C.ID CLAIM_ID,C.SERVICEDATE , O.STATE, O.ZIP,C.STATUS1
 FROM {{ ref('dim_claims')}} C 
 JOIN {{ ref('fct_encounters')}} E
   ON C.PATIENTID = E.PATIENT
  AND C.PROVIDERID = E.PROVIDER
  AND C.APPOINTMENTID =E.ID
 JOIN {{ ref('dim_organizations')}} O
   ON E.ORGANIZATION = O.ID
 ),
clm_amt as (
    select s.SERVICEDATE,s.STATE,s.ZIP,s.STATUS1 STATUS,sum(ct.payments) payments
      from source_data s
      join {{ ref('fct_claims_transactions')}} ct
        on s.CLAIM_ID = ct.CLAIMID
     group by all
  )
select * 
from clm_amt