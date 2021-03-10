WITh estimated_value AS (
  
    SELECT *
    FROM {{ref('mart_monthly_counter_adoption') }}
  
)

, base AS (
  
    SELECT *
    FROM {{ref('monthly_usage_data')}}
    WHERE TRUE
      AND is_smau

), fct_usage_ping_payloads AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}

), smau AS (

    SELECT 
      created_month,
      clean_metrics_name,
      edition,
      product_tier,
      group_name,
      stage_name,
      section_name, 
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      ping_source,
      SUM(monthly_metric_value) AS monthly_metric_value_sum
    FROM base
    INNER JOIN fct_usage_ping_payloads
      ON base.ping_id = fct_usage_ping_payloads.usage_ping_id
    {{ dbt_utils.group_by(n=12) }}



), smau_joined AS (

    SELECT 
      smau.*,
      ping_source        AS delivery,
      product_tier NOT IN ('Core', 'CE') AND ping_source = 'Self-Managed' AS is_paid,
      COALESCE(estimated_value.pct_subscriptions_with_counters, 1) AS pct_subscriptions_with_counters
    FROM smau
    LEFT JOIN estimated_value
      ON estimated_value.is_smau
        AND smau.ping_source = 'Self-Managed'
        AND smau.created_month  = estimated_value.reporting_month
        AND smau.stage_name = estimated_value.stage_name 
        AND smau.section_name = estimated_value.section_name
        AND smau.edition = estimated_value.edition
  
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
      SUM(monthly_metric_value_sum)                                        AS recorded_monthly_metric_value_sum,
      SUM(monthly_metric_value_sum) / MAX(pct_subscriptions_with_counters) AS estimated_monthly_metric_value_sum
    FROM smau_joined
    WHERE is_smau
    GROUP BY 1,2,3,4,5,6,7,8

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
      SUM(recorded_monthly_metric_value_sum)                            AS recorded_monthly_metric_value_sum,
      SUM(estimated_monthly_metric_value_sum)                           AS estimated_monthly_metric_value_sum
    FROM estimated_monthly_metric_value_sum
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
      0                                                                           AS recorded_monthly_metric_value_sum,
      SUM(estimated_monthly_metric_value_sum - recorded_monthly_metric_value_sum) AS estimated_monthly_metric_value_sum
    FROM estimated_monthly_metric_value_sum
    WHERE delivery = 'Self-Managed'
    GROUP BY 1,2,3,4,5,6,7,8
  
)

SELECT * 
FROM combined
