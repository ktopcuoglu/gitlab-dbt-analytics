{{ config({
    "materialized": "incremental",
    "unique_key": "daily_usage_data_event_id"
    })
}}


WITH usage_data AS (

    SELECT *
    FROM {{ ref('fct_event_all') }}
    {% if is_incremental() %}

      WHERE event_created_at >= (SELECT MAX(DATEADD(day, -8, event_created_date)) FROM {{this}})

    {% endif %}

)

, aggregated AS (

    SELECT
      -- PRIMARY KEY
      {{ dbt_utils.surrogate_key(['ultimate_parent_namespace_id', 'dim_user_id', 'event_name', 'event_created_at']) }} AS daily_usage_data_event_id,
      
      -- FOREIGN KEY
      ultimate_parent_namespace_id,
      dim_user_id,
      event_name,
      TO_DATE(event_created_at)                                                                                        AS event_created_date,

      is_blocked_namespace,
      namespace_created_date,
      namespace_is_internal,
      user_created_date,
      DATEDIFF('day', namespace_created_date, event_created_date)                                                      AS days_since_namespace_creation,
      DATEDIFF('week', namespace_created_date, event_created_date)                                                     AS weeks_since_namespace_creation,
      DATEDIFF('day', user_created_date, event_created_date)                                                           AS days_since_user_creation,
      DATEDIFF('week', user_created_date, event_created_date)                                                          AS weeks_since_user_creation,
      COUNT(DISTINCT event_id)                                                                                         AS event_count
    FROM usage_data
    WHERE days_since_user_creation >= 0
    {{ dbt_utils.group_by(n=13) }}

)

SELECT *
FROM aggregated
