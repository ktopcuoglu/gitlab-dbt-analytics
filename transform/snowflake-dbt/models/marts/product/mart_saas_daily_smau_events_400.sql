{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('fct_daily_event_400','fct_daily_event_400'),
    ('map_saas_event_to_smau','map_saas_event_to_smau')
]) }}


, joined AS (

    SELECT 
      -- PRIMARY KEY
      {{ dbt_utils.surrogate_key(['ultimate_parent_namespace_id', 'dim_user_id', 'stage_name', 'event_created_date']) }} AS daily_usage_data_event_id,
      
      -- FOREIGN KEY
      ultimate_parent_namespace_id,
      dim_user_id,
      event_created_date, 
      stage_name,
      is_smau,
      is_blocked_namespace,
      namespace_created_date,
      namespace_is_internal,
      user_created_date,
      days_since_namespace_creation,
      weeks_since_namespace_creation,
      days_since_user_creation,
      weeks_since_user_creation,
      COUNT(*)                                                                                               AS event_count
    FROM fct_daily_event_400
    INNER JOIN map_saas_event_to_smau
      ON fct_daily_event_400.event_name = map_saas_event_to_smau.event_name 
    {{ dbt_utils.group_by(n=14) }}

)

SELECT *
FROM joined
