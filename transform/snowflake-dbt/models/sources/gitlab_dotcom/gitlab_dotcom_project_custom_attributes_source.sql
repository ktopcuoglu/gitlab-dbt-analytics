{{ config({
    "materialized": "incremental",
    "unique_key": "project_custom_attribute_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom','project_custom_attributes') }}
  {% if is_incremental() %}
  WHERE created_at IS NOT NULL
    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})
  {% endif %}

), renamed AS (
  
  SELECT
    id::NUMBER            AS project_custom_attribute_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    project_id::NUMBER    AS project_id,
    key::VARCHAR          AS group_custom_key,
    value::VARCHAR        AS group_custom_value
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
  

)

SELECT *
FROM renamed
ORDER BY updated_at