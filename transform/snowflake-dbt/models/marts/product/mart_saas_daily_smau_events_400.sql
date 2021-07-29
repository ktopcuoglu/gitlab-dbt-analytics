{{ simple_cte([
    ('fct_daily_event_400','fct_daily_event_400'),
    ('saas_event_to_smau','saas_event_to_smau')
]) }}


, joined AS (

    SELECT 
      -- PRIMARY KEY
      {{ dbt_utils.surrogate_key(['dim_namespace_id', 'dim_user_id', 'stage_name', 'event_created_date']) }} AS daily_usage_data_event_id,
      
      -- FOREIGN KEY
      dim_namespace_id,
      dim_user_id,
      event_name,
      event_created_date,

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
    INNER JOIN saas_event_to_smau
      ON fct_daily_event_400.event_name = fct_daily_event_400.event_name 

)

SELECT *
FROM joined
