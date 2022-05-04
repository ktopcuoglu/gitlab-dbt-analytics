{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "service_ping_instance_metric_id"
) }}

{{ simple_cte([
    ('mart_service_ping_instance_metric_7_day', 'mart_service_ping_instance_metric_7_day'),
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day'),
    ('mart_service_ping_instance_metric_all_time', 'mart_service_ping_instance_metric_all_time')
    ])

}}

, mart_7_day AS (

SELECT * FROM mart_service_ping_instance_metric_7_day
{% if is_incremental() %}
            AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
{% endif %}

), mart_28_day AS (

SELECT * FROM mart_service_ping_instance_metric_28_day
{% if is_incremental() %}
            AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
{% endif %}

), mart_all_time AS (

SELECT * FROM mart_service_ping_instance_metric_all_time
{% if is_incremental() %}
            AND ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
{% endif %}

)

SELECT * FROM mart_7_day
  UNION ALL
SELECT * FROM mart_28_day
  UNION ALL
SELECT * FROM mart_all_time
