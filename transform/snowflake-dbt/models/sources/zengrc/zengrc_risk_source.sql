WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'risks') }}

),

renamed AS (

    SELECT
      code::VARCHAR                     AS risk_code,
      created_at::TIMESTAMP             AS risk_created_at,
      custom_attributes::VARIANT        AS risk_custom_attributes,
      description::VARCHAR              AS risk_description,
      id::NUMBER                        AS risk_id,
      risk_vector_score_values::VARIANT AS risk_vector_score_values,
      status::VARCHAR                   AS risk_status,
      title::VARCHAR                    AS risk_title,
      type::VARCHAR                     AS zengrc_object_type,
      updated_at::TIMESTAMP             AS risk_updated_at,
      __loaded_at::TIMESTAMP            AS risk_loaded_at
    FROM source

)

SELECT *
FROM renamed