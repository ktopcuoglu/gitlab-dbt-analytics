{{ config(
    tags=["product", "mnpi_exception"],
) }}

{{ simple_cte([
    ('fct_ping_instance_metric', 'fct_ping_instance_metric'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric_rolling_24_months AS (

    SELECT
      fct_ping_instance_metric.ping_instance_metric_id,
      fct_ping_instance_metric.dim_ping_instance_id,
      fct_ping_instance_metric.metrics_path,
      fct_ping_instance_metric.metric_value,
      dim_ping_metric.time_frame,
      fct_ping_instance_metric.has_timed_out,
      fct_ping_instance_metric.dim_product_tier_id,
      fct_ping_instance_metric.dim_subscription_id,
      fct_ping_instance_metric.dim_location_country_id,
      fct_ping_instance_metric.dim_ping_date_id,
      fct_ping_instance_metric.dim_instance_id,
      fct_ping_instance_metric.dim_host_id,
      fct_ping_instance_metric.dim_installation_id,
      fct_ping_instance_metric.dim_license_id,
      fct_ping_instance_metric.ping_created_at,
      fct_ping_instance_metric.umau_value,
      fct_ping_instance_metric.dim_subscription_license_id,
      fct_ping_instance_metric.data_source
    FROM fct_ping_instance_metric
    LEFT JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    WHERE DATE_TRUNC(MONTH, fct_ping_instance_metric.ping_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE))

)

{{ dbt_audit(
    cte_ref="fct_ping_instance_metric_rolling_24_months",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-07-20",
    updated_date="2022-07-20"
) }}
