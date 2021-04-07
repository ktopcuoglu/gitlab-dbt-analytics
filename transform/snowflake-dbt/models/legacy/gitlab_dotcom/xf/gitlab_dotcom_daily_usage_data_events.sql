{{ config({
    "materialized": "incremental",
    "unique_key": "daily_usage_data_event_id"
    })
}}


WITH usage_data AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
    {% if is_incremental() %}

      WHERE event_created_at >= (SELECT MAX(DATEADD(day, -1, event_date)) FROM {{this}})

    {% endif %}

)

, aggregated AS (

    SELECT
      {{ dbt_utils.surrogate_key(['namespace_id', 'user_id', 'event_name', 'TO_DATE(event_created_at)']) }} AS daily_usage_data_event_id,
      namespace_id,
      namespace_created_at,
      user_id,
      namespace_is_internal,
      is_representative_of_stage,
      event_name,
      stage_name,
      plan_id_at_event_date,
      plan_name_at_event_date,
      user_created_at,
      TO_DATE(event_created_at)                                         AS event_date,
      DATEDIFF('day', TO_DATE(namespace_created_at), event_date)        AS days_since_namespace_creation,
      DATEDIFF('week', TO_DATE(namespace_created_at), event_date)       AS weeks_since_namespace_creation,
      DATEDIFF('day', TO_DATE(user_created_at), event_date)             AS days_since_user_creation,
      DATEDIFF('week', TO_DATE(user_created_at), event_date)            AS weeks_since_user_creation,
      COUNT(*)                                                          AS event_count
    FROM usage_data
    WHERE days_since_user_creation >= 0
    {{ dbt_utils.group_by(n=16) }}

)

SELECT *
FROM aggregated
