with source_data as (
select Id , PATIENTID , PROVIDERID , PRIMARYPATIENTINSURANCEID , SECONDARYPATIENTINSURANCEID , DEPARTMENTID , 
PATIENTDEPARTMENTID , DIAGNOSIS1 , DIAGNOSIS2 , DIAGNOSIS3 , DIAGNOSIS4 , DIAGNOSIS5 , DIAGNOSIS6 , 
DIAGNOSIS7 , DIAGNOSIS8 , REFERRINGPROVIDERID , APPOINTMENTID , CURRENTILLNESSDATE , SERVICEDATE , 
SUPERVISINGPROVIDERID , STATUS1 , STATUS2 , STATUSP , OUTSTANDING1 , OUTSTANDING2 , OUTSTANDINGP , 
LASTBILLEDDATE1 , LASTBILLEDDATE2 , LASTBILLEDDATEP , HEALTHCARECLAIMTYPEID1 , HEALTHCARECLAIMTYPEID2 
  from {{ source('raw','hmsclaimsdata')}}
)
select *
from source_data