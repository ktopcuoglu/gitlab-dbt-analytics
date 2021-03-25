{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('gainsight_wave_2_3_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('fct_saas_usage_ping_namespace','fct_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('bdg_namespace_subscription','bdg_namespace_order_subscription_active'),
    ('gainsight_wave_2_3_metrics','gainsight_wave_2_3_metrics')
]) }}

, joined AS (
    SELECT 
      fct_saas_usage_ping_namespace.*, 
      first_day_of_month AS reporting_month, 
      dim_subscription_id, 
      subscription_name, 
      MAX(ping_date) OVER (
        PARTITION BY fct_saas_usage_ping_namespace.dim_namespace_id, first_day_of_month, dim_subscription_id, subscription_name, ping_name
        ) AS latest_usage_ping_date
    FROM fct_saas_usage_ping_namespace
    INNER JOIN dim_date ON fct_saas_usage_ping_namespace.ping_date = dim_date.date_day
    INNER JOIN bdg_namespace_subscription
      ON fct_saas_usage_ping_namespace.dim_namespace_id = bdg_namespace_subscription.dim_namespace_id
      AND namespace_order_subscription_match_status = 'Paid All Matching'
    INNER JOIN gainsight_wave_2_3_metrics
      ON fct_saas_usage_ping_namespace.ping_name = gainsight_wave_2_3_metrics.metric_name

), last_ping_in_month AS (

    SELECT *
    FROM joined
    WHERE latest_usage_ping_date = ping_date

), pivoted AS (

    SELECT
      dim_namespace_id,
      dim_subscription_id,
      reporting_month,
      {{ dbt_utils.pivot('ping_name', gainsight_wave_metrics, then_value='counter_value') }}
    FROM joined

)

SELECT *
FROM pivoted
