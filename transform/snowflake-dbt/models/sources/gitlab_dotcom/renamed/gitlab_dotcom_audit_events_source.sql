WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_audit_events_dedupe_source') }}
  
  {% if is_incremental() %}

  WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

  {% endif %}
  
), renamed AS (

  SELECT
    id::NUMBER              AS audit_event_id,
    author_id::NUMBER       AS author_id,
    entity_id::NUMBER       AS entity_id,
    entity_type::VARCHAR    AS entity_type,
    details::VARCHAR        AS audit_event_details,
    created_at::TIMESTAMP   AS created_at
  FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
