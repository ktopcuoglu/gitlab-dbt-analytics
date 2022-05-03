{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ macro_mrt_srvc_ping_instance_metric('dim_subscription') }}
