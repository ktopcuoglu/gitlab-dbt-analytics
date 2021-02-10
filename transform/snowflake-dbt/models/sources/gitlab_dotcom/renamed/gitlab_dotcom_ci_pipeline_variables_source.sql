WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_variables_dedupe_source') }}

), renamed AS (

    SELECT 
      id::NUMBER          AS ci_pipeline_variable_id, 
      key                  AS key, 
      pipeline_id::NUMBER AS ci_pipeline_id, 
      variable_type        AS variable_type

    FROM source

)


SELECT *
FROM renamed
