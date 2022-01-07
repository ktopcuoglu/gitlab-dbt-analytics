{{ config(
    materialized = "incremental"
) }}

SELECT
  ci_build_id,
  ci_build_user_id,
  created_at,
  ci_build_project_id
FROM {{ ref('gitlab_dotcom_ci_builds') }}
WHERE created_at IS NOT NULL
  AND created_at >= DATEADD(YEAR, -2, CURRENT_DATE)

  {% if is_incremental() %}

    AND created_at > (SELECT MAX(created_at) FROM {{ this }})

  {% endif %}
