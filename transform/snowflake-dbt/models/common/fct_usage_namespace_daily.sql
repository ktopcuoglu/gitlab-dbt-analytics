{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_usage_event_with_valid_user', 'fct_usage_event_with_valid_user')
    ])

}},

fct_usage_namespace_daily AS (
    
    SELECT 
      {{ dbt_utils.surrogate_key(['event_date', 'event_name', 'dim_ultimate_parent_namespace_id']) }}       
                                            AS usage_namespace_daily_id,
      dim_active_product_tier_id,
      dim_active_subscription_id,
      dim_crm_account_id,
      dim_billing_account_id, 
      dim_ultimate_parent_namespace_id, 
      dim_event_date_id, 
      plan_id_at_event_date,
      plan_name_at_event_date,
      plan_was_paid_at_event_date,
      days_since_namespace_creation_at_event_date,                            
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
    FROM fct_usage_event_with_valid_user
    {{ dbt_utils.group_by(n=20) }}
        
)

{{ dbt_audit(
    cte_ref="fct_usage_namespace_daily",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-04-09"
) }}
