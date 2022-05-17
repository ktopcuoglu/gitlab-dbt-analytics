{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id"
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('fct_ping_instance_metric', 'fct_ping_instance_metric')
    ])

}},

final AS (

  SELECT
    fct_ping_instance_metric.ping_instance_metric_id AS ping_instance_metric_id,
    fct_ping_instance_metric.dim_ping_instance_id AS dim_ping_instance_id,
    fct_ping_instance_metric.metrics_path AS metrics_path,
    fct_ping_instance_metric.metric_value AS metric_value,
    fct_ping_instance_metric.has_timed_out AS has_timed_out,
    fct_ping_instance_metric.dim_product_tier_id AS dim_product_tier_id,
    fct_ping_instance_metric.dim_subscription_id AS dim_subscription_id,
    fct_ping_instance_metric.dim_location_country_id AS dim_location_country_id,
    fct_ping_instance_metric.dim_ping_date_id AS dim_ping_date_id,
    fct_ping_instance_metric.dim_instance_id AS dim_instance_id,
    fct_ping_instance_metric.dim_host_id AS dim_host_id,
    fct_ping_instance_metric.dim_installation_id AS dim_installation_id,
    fct_ping_instance_metric.dim_license_id AS dim_license_id,
    fct_ping_instance_metric.ping_created_at AS ping_created_at,
    fct_ping_instance_metric.umau_value AS umau_value,
    fct_ping_instance_metric.dim_subscription_license_id AS dim_subscription_license_id,
    fct_ping_instance_metric.data_source AS data_source,
    fct_ping_instance_metric.time_frame AS time_frame
  FROM fct_ping_instance_metric
  INNER JOIN dim_ping_instance
    ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
  WHERE time_frame IN('28d', 'all')
    AND is_last_ping_of_month = TRUE
    AND has_timed_out = FALSE
    AND metric_value IS NOT NULL

  {% if is_incremental() %}
                
    AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    
  {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-05-09",
    updated_date="2022-05-12"
) }}
