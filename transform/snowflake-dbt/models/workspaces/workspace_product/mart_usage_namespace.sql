{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_usage_event', 'mart_usage_event'),
    ])
}}

WITH usage_events AS (
    SELECT
        {{ dbt_utils.surrogate_key(['event_date', 'event_name', 'dim_namespace_id','plan_was_paid_at_event_date']) }}       AS mart_usage_namespace_id,
        event_date,
        event_name,
        dim_product_tier_id,
        dim_subscription_id,
        dim_crm_account_id,
        dim_billing_account_id,
        stage_name,
        section_name,
        group_name,
        source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_instance_id,
        dim_namespace_id,
        namespace_created_at,
        days_since_namespace_created,
        is_smau,
        is_gmau,
        is_umau,
        COUNT(*) AS event_count
    FROM mart_usage_event
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
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
