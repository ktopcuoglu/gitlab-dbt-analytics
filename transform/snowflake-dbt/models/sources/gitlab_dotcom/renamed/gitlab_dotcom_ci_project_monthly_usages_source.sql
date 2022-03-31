WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_project_monthly_usages_dedupe_source') }}
      
)

, renamed AS (
  
    SELECT
      id::NUMBER                        AS ci_project_monthly_usages_id,
      project_id::NUMBER                AS project_id,
      date::TIMESTAMP                   AS date,
      amount_used::NUMBER               AS amount_used,
      shared_runners_duration::NUMBER   AS shared_runners_duration,
      created_at::TIMESTAMP             AS created_at
    FROM source
  
)

SELECT * 
FROM renamed