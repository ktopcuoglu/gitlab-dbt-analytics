{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
        "materialized": "table",
        "schema": "common_mart_product",
        "unique_key": "primary_key"
    })
}}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('estimated_value','mart_monthly_counter_adoption'),
    ('fct_usage_ping_payload','fct_usage_ping_payload'),
    ('fct_monthly_usage_data','fct_monthly_usage_data'),
    ('fct_daily_event_400','fct_daily_event_400'),
    ('map_saas_event_to_gmau','map_saas_event_to_gmau'),
    ('map_saas_event_to_smau','map_saas_event_to_smau')
]) }}

, smau AS (

    SELECT 
      ping_created_month,
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
      usage_ping_delivery_type,
      SUM(monthly_metric_value) AS monthly_metric_value_sum
    FROM fct_monthly_usage_data
    INNER JOIN fct_usage_ping_payload
      ON fct_monthly_usage_data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
    WHERE is_smau = TRUE
      AND product_tier <> 'Core'
      AND usage_ping_delivery_type = 'Self-Managed'
    {{ dbt_utils.group_by(n=12) }}



), smau_joined AS (

    SELECT 
      smau.*,
      usage_ping_delivery_type                                                         AS delivery,
      'SMAU'                                                              AS xmau_level,
      product_tier NOT IN ('Core', 'CE') AND usage_ping_delivery_type = 'Self-Managed' AS is_paid,
      COALESCE(estimated_value.pct_subscriptions_with_counters, 1)        AS pct_subscriptions_with_counters
    FROM smau
    LEFT JOIN estimated_value
      ON estimated_value.is_smau
        AND smau.usage_ping_delivery_type = 'Self-Managed'
        AND smau.ping_created_month  = estimated_value.reporting_month
        AND smau.stage_name = estimated_value.stage_name 
        AND smau.section_name = estimated_value.section_name
        AND smau.edition = estimated_value.edition
  
), saas_smau AS (
  
    SELECT 
      first_day_of_month                AS reporting_month,
      'SaaS'                            AS delivery,
      NULL                              AS section_name,
      stage_name,
      NULL                              AS group_name,
      'SMAU'                            AS xmau_level,
      'SaaS'                            AS product_tier,
      'SaaS'                            AS edition,
      COUNT(DISTINCT dim_user_id)       AS recorded_monthly_metric_value_sum,
      recorded_monthly_metric_value_sum AS estimated_monthly_metric_value_sum

    FROM fct_daily_event_400
    INNER JOIN map_saas_event_to_smau
      ON fct_daily_event_400.event_name = map_saas_event_to_smau.event_name
    INNER JOIN dim_date
      ON fct_daily_event_400.event_created_date = dim_date.date_day
      AND DATEDIFF('day', event_created_date, last_day_of_month) < 28
    WHERE fct_daily_event_400.dim_plan_id_at_event_date <> 34
    {{ dbt_utils.group_by(n=8) }}


), umau AS (

    SELECT 
      ping_created_month,
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
      usage_ping_delivery_type,
      SUM(monthly_metric_value) AS monthly_metric_value_sum
    FROM fct_monthly_usage_data
    INNER JOIN fct_usage_ping_payload
      ON fct_monthly_usage_data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
    WHERE is_umau = TRUE
      AND product_tier <> 'Core'
      AND usage_ping_delivery_type = 'Self-Managed'
    {{ dbt_utils.group_by(n=12) }}



), umau_joined AS (

    SELECT 
      umau.*,
      usage_ping_delivery_type                                                         AS delivery,
      'UMAU'                                                              AS xmau_level,
      product_tier NOT IN ('Core', 'CE') AND usage_ping_delivery_type = 'Self-Managed' AS is_paid,
      COALESCE(estimated_value.pct_subscriptions_with_counters, 1)        AS pct_subscriptions_with_counters
    FROM umau
    LEFT JOIN estimated_value
      ON estimated_value.is_umau
        AND umau.usage_ping_delivery_type = 'Self-Managed'
        AND umau.ping_created_month  = estimated_value.reporting_month
        AND umau.edition = estimated_value.edition
  
), instance_gmau AS (

    SELECT 
      ping_created_month,
      clean_metrics_name,
      fct_monthly_usage_data.host_name,
      fct_monthly_usage_data.dim_instance_id,
      edition,
      product_tier,
      group_name,
      stage_name,
      section_name, 
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      usage_ping_delivery_type,
      MAX(monthly_metric_value) AS monthly_metric_value
    FROM fct_monthly_usage_data
    INNER JOIN fct_usage_ping_payload
      ON fct_monthly_usage_data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
    WHERE ((is_paid_gmau = TRUE
              AND usage_ping_delivery_type = 'Self-Managed'
    ) OR (is_paid_gmau = TRUE and is_gmau = FALSE)) -- if a specific paid_gmau metric has beeen creeated we don't need to exclude SaaS
      AND product_tier <> 'Core'

    {{ dbt_utils.group_by(n=14) }}



), gmau AS (

    SELECT 
      ping_created_month,
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
      usage_ping_delivery_type,
      SUM(monthly_metric_value) AS monthly_metric_value_sum
    FROM instance_gmau
    WHERE is_paid_gmau = TRUE
    {{ dbt_utils.group_by(n=12) }}

), gmau_joined AS (

    SELECT 
      gmau.*,
      usage_ping_delivery_type                                                         AS delivery,
      'GMAU'                                                                           AS xmau_level,
      product_tier NOT IN ('Core', 'CE') AND usage_ping_delivery_type = 'Self-Managed' AS is_paid,
      COALESCE(MAX(estimated_value.pct_subscriptions_with_counters), 1)                AS pct_subscriptions_with_counters
    FROM gmau
    LEFT JOIN estimated_value 
      ON estimated_value.is_paid_gmau
        AND gmau.usage_ping_delivery_type = 'Self-Managed'
        AND gmau.ping_created_month  = estimated_value.reporting_month
        AND gmau.stage_name = estimated_value.stage_name 
        AND gmau.group_name = estimated_value.group_name 
        AND gmau.section_name = estimated_value.section_name
        AND gmau.edition = estimated_value.edition
    {{ dbt_utils.group_by(n=15) }}
  
), saas_gmau AS (
  
    SELECT 
      first_day_of_month                AS reporting_month,
      'SaaS'                            AS delivery,
      NULL                              AS section_name,
      stage_name,
      group_name,
      'GMAU'                            AS xmau_level,
      'SaaS'                            AS product_tier,
      'SaaS'                            AS edition,
      COUNT(DISTINCT dim_user_id)       AS recorded_monthly_metric_value_sum,
      recorded_monthly_metric_value_sum AS estimated_monthly_metric_value_sum

    FROM fct_daily_event_400
    INNER JOIN map_saas_event_to_gmau
      ON fct_daily_event_400.event_name = map_saas_event_to_gmau.event_name
    INNER JOIN dim_date
      ON fct_daily_event_400.event_created_date = dim_date.date_day
      AND DATEDIFF('day', event_created_date, last_day_of_month) < 28
    WHERE fct_daily_event_400.dim_plan_id_at_event_date <> 34
    {{ dbt_utils.group_by(n=8) }}

), xmau AS (

    SELECT *
    FROM gmau_joined

    UNION 

    SELECT *
    FROM smau_joined

    UNION 

    SELECT *
    FROM umau_joined
    
), estimated_monthly_metric_value_sum AS (

    SELECT 
      ping_created_month::DATE                                             AS reporting_month,
      delivery,
      xmau_level,
      is_smau,
      section_name,
      stage_name,
      group_name,
      IFF(delivery='SaaS', delivery, product_tier)                         AS product_tier,
      IFF(delivery='SaaS', delivery, edition)                              AS edition,
      'version'       AS data_source,
      SUM(monthly_metric_value_sum)                                        AS recorded_monthly_metric_value_sum,
      SUM(monthly_metric_value_sum) / MAX(pct_subscriptions_with_counters) AS estimated_monthly_metric_value_sum
    FROM xmau
    {{ dbt_utils.group_by(n=10) }}

), combined AS (

    SELECT
      reporting_month,
      section_name,
      stage_name,
      group_name,
      product_tier,
      xmau_level,
      IFF(delivery = 'Self-Managed', 'Recorded Self-Managed', delivery) AS breakdown,
      delivery,
      edition,
      SUM(recorded_monthly_metric_value_sum)                            AS recorded_monthly_metric_value_sum,
      -- this is expected as the breakdown is for Recorded Self-Managed and Saas
      -- Estimated Uplift being calculated in the next unioned table
      SUM(recorded_monthly_metric_value_sum)                            AS estimated_monthly_metric_value_sum
    FROM estimated_monthly_metric_value_sum
    {{ dbt_utils.group_by(n=9) }}

    UNION 

    SELECT
      reporting_month,
      section_name,
      stage_name,
      group_name,
      product_tier,
      xmau_level,
      'Estimated Self-Managed Uplift' AS breakdown,
      delivery,
      edition,
      0                                                                           AS recorded_monthly_metric_value_sum,
      -- calculating Estimated Uplift here
      SUM(estimated_monthly_metric_value_sum - recorded_monthly_metric_value_sum) AS estimated_monthly_metric_value_sum
    FROM estimated_monthly_metric_value_sum
    WHERE delivery = 'Self-Managed'
    {{ dbt_utils.group_by(n=9) }}
  
    UNION 

    SELECT
      reporting_month,
      section_name,
      stage_name,
      group_name,
      product_tier,
      xmau_level,
      'SaaS' AS breakdown,
      delivery,
      edition,
      recorded_monthly_metric_value_sum,
      estimated_monthly_metric_value_sum
    FROM saas_gmau
  
    UNION 

    SELECT
      reporting_month,
      section_name,
      stage_name,
      group_name,
      product_tier,
      xmau_level,
      'SaaS' AS breakdown,
      delivery,
      edition,
      recorded_monthly_metric_value_sum,
      estimated_monthly_metric_value_sum
    FROM saas_smau

)

SELECT * 
FROM combined
