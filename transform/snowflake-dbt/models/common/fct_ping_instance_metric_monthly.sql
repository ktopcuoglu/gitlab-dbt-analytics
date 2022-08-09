{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, fct_ping_instance_metric AS (

    SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
      TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
    FROM {{ ref('fct_ping_instance_metric') }}

),

time_frame_28_day_metrics AS (

    SELECT
      fct_ping_instance_metric.*,
      fct_ping_instance_metric.metric_value AS monthly_metric_value,
      dim_ping_metric.time_frame
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    WHERE time_frame = ('28d')
      AND is_last_ping_of_month = TRUE
      AND has_timed_out = FALSE
      AND metric_value IS NOT NULL

),

time_frame_all_time_metrics AS (

    SELECT
      fct_ping_instance_metric.*,
      {{ monthly_all_time_metric_calc('fct_ping_instance_metric.metric_value', 'fct_ping_instance_metric.dim_installation_id',
                                    'fct_ping_instance_metric.metrics_path', 'fct_ping_instance_metric.ping_created_at') }},
      dim_ping_metric.time_frame
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    WHERE time_frame = ('all')
      AND is_last_ping_of_month = TRUE
      AND has_timed_out = FALSE
      AND metric_value IS NOT NULL
      AND typeof(metric_value) IN ('INTEGER', 'DECIMAL')

),

final AS (

    SELECT
      time_frame_28_day_metrics.ping_instance_metric_id,
      time_frame_28_day_metrics.dim_ping_instance_id,
      time_frame_28_day_metrics.dim_product_tier_id,
      time_frame_28_day_metrics.dim_subscription_id,
      time_frame_28_day_metrics.dim_location_country_id,
      time_frame_28_day_metrics.dim_ping_date_id,
      time_frame_28_day_metrics.dim_instance_id,
      time_frame_28_day_metrics.dim_host_id,
      time_frame_28_day_metrics.dim_installation_id,
      time_frame_28_day_metrics.dim_license_id,
      time_frame_28_day_metrics.dim_subscription_license_id,
      time_frame_28_day_metrics.metrics_path,
      time_frame_28_day_metrics.metric_value,
      time_frame_28_day_metrics.monthly_metric_value,
      time_frame_28_day_metrics.time_frame,
      time_frame_28_day_metrics.has_timed_out,
      time_frame_28_day_metrics.ping_created_at,
      time_frame_28_day_metrics.umau_value,
      time_frame_28_day_metrics.data_source
    FROM time_frame_28_day_metrics

    UNION ALL

    SELECT
      time_frame_all_time_metrics.ping_instance_metric_id,
      time_frame_all_time_metrics.dim_ping_instance_id,
      time_frame_all_time_metrics.dim_product_tier_id,
      time_frame_all_time_metrics.dim_subscription_id,
      time_frame_all_time_metrics.dim_location_country_id,
      time_frame_all_time_metrics.dim_ping_date_id,
      time_frame_all_time_metrics.dim_instance_id,
      time_frame_all_time_metrics.dim_host_id,
      time_frame_all_time_metrics.dim_installation_id,
      time_frame_all_time_metrics.dim_license_id,
      time_frame_all_time_metrics.dim_subscription_license_id,
      time_frame_all_time_metrics.metrics_path,
      time_frame_all_time_metrics.metric_value,
      IFF(time_frame_all_time_metrics.monthly_metric_value < 0, 0, time_frame_all_time_metrics.monthly_metric_value) AS monthly_metric_value,
      time_frame_all_time_metrics.time_frame,
      time_frame_all_time_metrics.has_timed_out,
      time_frame_all_time_metrics.ping_created_at,
      time_frame_all_time_metrics.umau_value,
      time_frame_all_time_metrics.data_source
    FROM time_frame_all_time_metrics

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-05-09",
    updated_date="2022-07-20"
) }}
