WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'controls') }}

),

renamed AS (

    SELECT
      code::VARCHAR               AS control_code,
      created_at::TIMESTAMP       AS control_created_at,
      description::VARCHAR        AS control_description,
      id::NUMBER                  AS control_id,
      mapped__objectives::VARIANT AS mapped_objectives,
      status::VARCHAR             AS control_status,
      title::VARCHAR              AS control_title,
      type::VARCHAR               AS zengrc_object_type,
      updated_at::TIMESTAMP       AS control_updated_at,
      __loaded_at::TIMESTAMP      AS control_loaded_at
    FROM source

)

SELECT *
FROM renamed



