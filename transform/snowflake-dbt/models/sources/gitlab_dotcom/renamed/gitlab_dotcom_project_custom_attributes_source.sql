WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_custom_attributes_dedupe_source') }}

), renamed AS (
  
  SELECT
    id::NUMBER            AS project_custom_attribute_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    project_id::NUMBER    AS project_id,
    key::VARCHAR          AS project_custom_key,
    value::VARCHAR        AS project_custom_value
  FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at