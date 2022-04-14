{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_usage_event', 'mart_usage_event'),
    ])
}},

mart_usage_namespace_daily AS (
    
    SELECT 
      {{ dbt_utils.surrogate_key(['event_date', 'event_name', 'dim_ultimate_parent_namespace_id']) }}       
                                            AS mart_usage_namespace_id,
      dim_active_product_tier_id,
      dim_active_subscription_id,
      dim_crm_account_id,
      dim_billing_account_id, 
      dim_ultimate_parent_namespace_id,  
      plan_id_at_event_date,
      plan_name_at_event_date,
      plan_was_paid_at_event_date,
      namespace_created_at,                             
      event_date,
      event_name,
      stage_name,
      section_name,
      group_name,
      data_source,
      is_smau,
      is_gmau,
      is_umau,
      COUNT(*) AS event_count,
      COUNT(DISTINCT(dim_user_id)) AS user_count
    FROM mart_usage_event
    WHERE dim_user_id IS NOT NULL
    {{ dbt_utils.group_by(n=19) }}
        
)

{{ dbt_audit(
    cte_ref="mart_usage_namespace_daily",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-02-15",
    updated_date="2022-04-09"
) }}
