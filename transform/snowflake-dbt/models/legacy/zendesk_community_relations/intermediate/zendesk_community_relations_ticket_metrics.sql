WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_community_relations_ticket_metrics_source') }}

),

renamed AS (

    SELECT
      *,
      reply_time_in_minutes_during_calendar_hours  AS sla_reply_time_calendar_hours,
      reply_time_in_minutes_during_business_hours  AS sla_reply_time_business_hours

    FROM source

)

SELECT *
FROM renamed
