{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id"
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric_7_day') }}
