{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_usage_event_with_valid_user', 'fct_usage_event_with_valid_user')
    ])

}},

fct_usage_instance_daily AS (
    
  SELECT
    {{ dbt_utils.surrogate_key(['event_date', 'event_name']) }} AS usage_instance_daily_id,
    event_date,
    event_name,
    data_source,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count
  FROM fct_usage_event_with_valid_user
  {{ dbt_utils.group_by(n=4) }}
  
)

{{ dbt_audit(
    cte_ref="fct_usage_instance_daily",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-04-09"
) }}
