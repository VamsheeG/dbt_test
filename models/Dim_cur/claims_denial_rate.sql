{{ config(materialized='view')}}

WITH source_data AS
(SELECT zip organization_zip, count(*) total_claims,
        sum(case when status='REJECTED'  then 1 else 0 end) rejected_claims
   FROM {{ ref('claims_state_daily_amount') }} E
   group by 1)
SELECT organization_zip,total_claims,rejected_claims, (rejected_claims/total_claims)*100::number as rejected_percent
 from source_data

