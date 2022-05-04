{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "service_ping_instance_metric_id"
) }}

{{ macro_mart_service_ping_instance_metric('fct_service_ping_instance_metric_7_day') }}
