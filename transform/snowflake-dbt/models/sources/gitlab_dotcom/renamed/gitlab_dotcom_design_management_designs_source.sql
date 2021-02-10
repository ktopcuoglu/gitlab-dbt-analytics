WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_design_management_designs_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                                 AS design_id,
      project_id::NUMBER                         AS project_id,
      issue_id::NUMBER                           AS issue_id,
      filename::VARCHAR                           AS design_filename
    FROM source

)

SELECT *
FROM renamed
