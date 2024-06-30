with source_data as (
select  ID , CLAIMID , CHARGEID , PATIENTID , TYPE , AMOUNT , METHOD , FROMDATE , TODATE , PLACEOFSERVICE , 
PROCEDURECODE , MODIFIER1 , MODIFIER2 , DIAGNOSISREF1 , DIAGNOSISREF2 , DIAGNOSISREF3 , DIAGNOSISREF4 , 
UNITS , DEPARTMENTID , NOTES , UNITAMOUNT , TRANSFEROUTID , TRANSFERTYPE , PAYMENTS , ADJUSTMENTS , 
TRANSFERS , OUTSTANDING , APPOINTMENTID , LINENOTE , PATIENTINSURANCEID , FEESCHEDULEID , PROVIDERID , 
SUPERVISINGPROVIDERID 
  from {{ source('raw','hmsclaimstransdata')}}
)
select *
from source_data