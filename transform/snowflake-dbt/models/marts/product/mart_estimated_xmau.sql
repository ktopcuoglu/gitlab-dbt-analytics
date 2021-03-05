WITh estimated_value AS (
  
  SELECT *
  FROM {{ref('mart_monthly_counter_adoption') }}
  
)

, base AS (
  
  SELECT *
  FROM {{ref('mart_poc_monthly_usage_data')}}
  WHERE TRUE
    AND is_smau


), mart_saas_xmau AS (
  
  SELECT *
  FROM {{ref('wip_mart_saas_xmau')}}

), base_joined AS (

  SELECT 
    base.*,
    ping_source        AS delivery,
    product_tier NOT IN ('Core', 'CE') AND ping_source = 'Self-Managed' AS is_paid,
    COALESCE(estimated_value.pct_subscriptions_with_counters, 1) AS pct_subscriptions_with_counters
  FROM base
  LEFT JOIN estimated_value
    ON base.created_month  = estimated_value.reporting_month
      AND estimated_value.is_smau AND base.stage_name = estimated_value.stage_name AND base.section_name = estimated_value.section_name
      AND ping_source = 'Self-Managed'
      AND base.edition = estimated_value.edition
  
), estimated_monthly_metric_value_sum AS (

SELECT 
  created_month::DATE                                                  AS reporting_month,
  delivery,
  is_smau,
  section_name,
  stage_name,
  NULL               AS group_name,
  IFF(delivery='SaaS', delivery, product_tier)                         AS product_tier,
  IFF(delivery='SaaS', delivery, edition)                              AS edition,
  'version'       AS data_source,
  SUM(monthly_metric_value_sum)                                        AS monthly_metric_value_sum,
  SUM(monthly_metric_value_sum) / MAX(pct_subscriptions_with_counters) AS estimated_monthly_metric_value_sum
FROM base_joined
WHERE is_smau
GROUP BY 1,2,3,4,5,6,7,8

), saas_monthly_metric_value_renamed AS ( 

SELECT 
  first_day_of_month AS reporting_month,
  'SaaS' AS delivery,
  is_smau,
  section_name,
  stage_name,
  NULL                AS group_name,
  plan_name_at_event_date AS product_tier,
  'SaaS' AS edition,
  'postgres_db'       AS data_source,
  mau_metric_value,
  mau_metric_value AS estimated_monthly_metric_value_sum

FROM mart_saas_xmau
WHERE TRUE
  AND is_smau
  AND first_day_of_month >= (SELECT MIN(created_month) FROM base_joined)
  
), saas_free AS (
  
  SELECT 
  estimated_monthly_metric_value_sum.reporting_month,
  estimated_monthly_metric_value_sum.delivery,
  estimated_monthly_metric_value_sum.is_smau,
  estimated_monthly_metric_value_sum.section_name,
  estimated_monthly_metric_value_sum.stage_name,
  NULL               AS group_name,
  'SaaS Core'                    AS product_tier,
  estimated_monthly_metric_value_sum.edition,
  'version'       AS data_source,
  SUM(estimated_monthly_metric_value_sum.monthly_metric_value_sum) - SUM(mau_metric_value),
  SUM(estimated_monthly_metric_value_sum.monthly_metric_value_sum) - SUM(mau_metric_value)
  FROM estimated_monthly_metric_value_sum
  LEFT JOIN saas_monthly_metric_value_renamed
    ON estimated_monthly_metric_value_sum.reporting_month = saas_monthly_metric_value_renamed.reporting_month
      AND estimated_monthly_metric_value_sum.stage_name = saas_monthly_metric_value_renamed.stage_name
      AND estimated_monthly_metric_value_sum.group_name = saas_monthly_metric_value_renamed.group_name
      AND estimated_monthly_metric_value_sum.is_smau = saas_monthly_metric_value_renamed.is_smau
  WHERE estimated_monthly_metric_value_sum.delivery = 'SaaS'
  GROUP BY 1,2,3,4,5,6,7,8

), unioned AS (
  
  SELECT *
  FROM estimated_monthly_metric_value_sum

  UNION

  SELECT *
  FROM saas_monthly_metric_value_renamed

  UNION

  SELECT *
  FROM saas_free

), combined AS (

SELECT
  reporting_month,
  section_name,
  stage_name,
  group_name,
  product_tier,
  IFF(delivery = 'Self-Managed', 'Recorded Self-Managed', delivery) AS breakdown,
  delivery,
  edition,
  SUM(monthly_metric_value_sum)  AS mau_value
FROM unioned
GROUP BY 1,2,3,4,5,6,7,8

UNION 

SELECT
  reporting_month,
  section_name,
  stage_name,
  group_name,
  product_tier,
  'Estimated Self-Managed Uplift' AS breakdown,
  delivery,
  edition,
  SUM(estimated_monthly_metric_value_sum - monthly_metric_value_sum) AS mau_value
FROM unioned
WHERE delivery = 'Self-Managed'
GROUP BY 1,2,3,4,5,6,7,8
  
)

SELECT * 
FROM combined
