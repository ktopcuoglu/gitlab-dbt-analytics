{{
  config({
    "materialized": "table"
  })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('gainsight_wave_2_3_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_saas_usage_ping_namespace','prep_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('bdg_namespace_subscription','bdg_namespace_order_subscription'),
    ('gainsight_wave_2_3_metrics','gainsight_wave_2_3_metrics')
]) }}

, joined AS (
    SELECT 
      prep_saas_usage_ping_namespace.*, 
      first_day_of_month AS reporting_month, 
      dim_subscription_id, 
      subscription_name, 
      MAX(ping_date) OVER (
        PARTITION BY prep_saas_usage_ping_namespace.dim_namespace_id, first_day_of_month, dim_subscription_id, subscription_name, ping_name
        ) AS latest_usage_ping_date
    FROM prep_saas_usage_ping_namespace
    INNER JOIN dim_date ON prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    INNER JOIN bdg_namespace_subscription
      ON prep_saas_usage_ping_namespace.dim_namespace_id = bdg_namespace_subscription.dim_namespace_id
      AND namespace_order_subscription_match_status = 'Paid All Matching'
    INNER JOIN gainsight_wave_2_3_metrics
      ON prep_saas_usage_ping_namespace.ping_name = gainsight_wave_2_3_metrics.metric_name

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
    {{ dbt_utils.group_by(n=3)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-03-22",
    updated_date="2021-04-06"
) }}