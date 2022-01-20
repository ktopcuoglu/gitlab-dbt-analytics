WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_control_source') }}

)

SELECT
  control_code,
  control_created_at,
  control_description,
  control_id,
  control_status,
  control_title,
  zengrc_object_type,
  control_updated_at,
  control_loaded_at
FROM source