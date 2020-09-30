{{ config({
        "materialized": "incremental"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_audit_events_source') }}
  {% if is_incremental() %}
  WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
  {% endif %}

), sequence AS (

    {{ dbt_utils.generate_series(upper_bound=11) }}

), details_parsed AS (

    SELECT
      audit_event_id,
      REGEXP_SUBSTR(audit_event_details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 1) AS key_name,
      REGEXP_SUBSTR(audit_event_details, '\\:([a-z_]*)\\: (.*)', 1, generated_number, 'c', 2) AS key_value,
      created_at
    FROM source
    INNER JOIN sequence
    WHERE key_name IS NOT NULL

)

SELECT *
FROM details_parsed
