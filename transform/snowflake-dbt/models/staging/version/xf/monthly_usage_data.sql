WITH monthly_usage_data_all_time AS (

  SELECT *
  FROM {{ ref('monthly_usage_data_all_time') }}

)

, monthly_usage_data_28_days AS (

  SELECT *
  FROM {{ ref('monthly_usage_data_all_time') }}

)

SELECT *
FROM monthly_usage_data_all_time
