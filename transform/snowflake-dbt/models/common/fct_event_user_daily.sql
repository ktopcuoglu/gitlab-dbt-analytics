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
  {{ dbt_utils.surrogate_key(['event_date', 'dim_user_id','dim_ultimate_parent_namespace_id','event_name']) }} 
                                                  AS event_user_daily_id,
    dim_active_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_user_id,
    dim_ultimate_parent_namespace_id,
    dim_event_date_id,
    event_date,
    event_name,
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
    COUNT(*) AS event_count
  FROM fct_event_valid
  WHERE dim_user_id IS NOT NULL
  {{ dbt_utils.group_by(n=20) }}

)

{{ dbt_audit(
    cte_ref="fct_event_user_daily",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-05-16"
) }}
