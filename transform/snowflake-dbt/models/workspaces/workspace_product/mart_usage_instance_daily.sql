{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_usage_event', 'mart_usage_event'),
    ])
}}

, usage_events AS (
    SELECT
        {{ dbt_utils.surrogate_key(['event_date', 'event_name', 'dim_instance_id']) }}       AS mart_usage_instance_id,
        event_date,
        event_name,
        data_source,
        dim_instance_id,
        COUNT(*) AS event_count,
        COUNT(DISTINCT(dim_user_id)) AS distinct_user_count
    FROM mart_usage_event
        {{ dbt_utils.group_by(n=5) }}
), results AS (

    SELECT *
    FROM usage_events

)


{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-02-15",
    updated_date="2022-02-16"
) }}
