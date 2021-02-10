WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_schedule_variables_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                   AS ci_pipeline_schedule_variable_id,
      key                           AS key,
      pipeline_schedule_id::NUMBER AS ci_pipeline_schedule_id,
      created_at::TIMESTAMP         AS created_at,
      updated_at::TIMESTAMP         AS updated_at,
      variable_type                 AS variable_type

    FROM source

)


SELECT *
FROM renamed
