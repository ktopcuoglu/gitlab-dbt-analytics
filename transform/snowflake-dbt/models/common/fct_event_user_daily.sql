{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event_valid', 'fct_event_valid')
    ])
}},

fct_event_user_daily AS (

  SELECT
    --Primary Key
    {{ dbt_utils.surrogate_key(['event_date', 'dim_user_id', 'dim_ultimate_parent_namespace_id', 'event_name']) }} 
                                                  AS event_user_daily_pk,
    
    --Foreign Keys                                               
    dim_active_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_user_sk,
    dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
    dim_ultimate_parent_namespace_id,
    dim_event_date_id,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    event_date,
    event_name,
    days_since_user_creation_at_event_date,
    days_since_namespace_creation_at_event_date,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date,
    stage_name,
    section_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    data_source,
    
    --Facts
    COUNT(*) AS event_count
    
  FROM fct_event_valid
  WHERE is_null_user = FALSE
  {{ dbt_utils.group_by(n=23) }}

)

{{ dbt_audit(
    cte_ref="fct_event_user_daily",
    created_by="@iweeks",
    updated_by="@tpoole1",
    created_date="2022-04-09",
    updated_date="2022-08-01"
) }}
