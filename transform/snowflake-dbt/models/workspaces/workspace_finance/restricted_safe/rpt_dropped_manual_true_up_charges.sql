WITH base_all_time AS (
  
  SELECT DISTINCT 
    subscription_name
  FROM {{ source('snapshots', 'mart_charge_snapshot') }}
  WHERE rate_plan_charge_name = 'manual true up allocation'

), base_live AS (

  SELECT DISTINCT 
    subscription_name
  FROM {{ ref('mart_charge') }}
  WHERE rate_plan_charge_name = 'manual true up allocation'

)

SELECT *
FROM base_all_time
WHERE subscription_name NOT IN (SELECT subscription_name FROM base_live)
