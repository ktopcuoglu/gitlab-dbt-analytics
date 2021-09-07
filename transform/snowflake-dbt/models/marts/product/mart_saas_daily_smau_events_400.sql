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
      fct_daily_event_400.ultimate_parent_namespace_id,
      fct_daily_event_400.dim_user_id,
      fct_daily_event_400.event_created_date, 
      map_saas_event_to_smau.stage_name,
      map_saas_event_to_smau.is_smau,
      map_saas_event_to_smau.is_umau,
      fct_daily_event_400.is_blocked_namespace,
      fct_daily_event_400.namespace_created_date,
      fct_daily_event_400.namespace_is_internal,
      fct_daily_event_400.user_created_date,
      fct_daily_event_400.days_since_namespace_creation,
      fct_daily_event_400.weeks_since_namespace_creation,
      fct_daily_event_400.days_since_user_creation,
      fct_daily_event_400.weeks_since_user_creation,
      COUNT(*)                                                                                                           AS event_count
    FROM fct_daily_event_400
    INNER JOIN map_saas_event_to_smau
      ON fct_daily_event_400.event_name = map_saas_event_to_smau.event_name 
    {{ dbt_utils.group_by(n=15) }}

)

SELECT *
FROM joined
