{{ config(
    materialized = "incremental"
) }}

SELECT
  note_author_id,
  project_id,
  note_id,
  created_at,
  noteable_type
FROM {{ ref('gitlab_dotcom_notes') }}
WHERE created_at IS NOT NULL
  AND created_at >= DATEADD(YEAR, -2, CURRENT_DATE)
  AND noteable_type IN (
    'Issue',
    'MergeRequest'
  )

  {% if is_incremental() %}

    AND created_at > (SELECT MAX(created_at) FROM {{ this }})

  {% endif %}
