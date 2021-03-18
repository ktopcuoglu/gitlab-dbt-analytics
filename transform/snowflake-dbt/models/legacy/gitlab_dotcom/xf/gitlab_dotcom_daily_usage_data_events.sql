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
      {{ dbt_utils.star(from=ref('gitlab_dotcom_usage_data_events'), except=["EVENT_CREATED_AT", "PARENT_TYPE", "PARENT_ID", "PARENT_CREATED_AT"]) }},
      TO_DATE(event_created_at) AS event_date,
      COUNT(*)                  AS event_count
    FROM usage_data
    {{ dbt_utils.group_by(n=18) }}

)

SELECT *
FROM aggregated
