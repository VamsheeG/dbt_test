{{ config(materialized='view')}}

WITH source_data AS
(SELECT state, sum(payments) payments 
   FROM {{ ref('claims_state_daily_amount') }} E
   where e.STATUS = 'CLOSED'
   group by 1)
SELECT state, payments
 from source_data