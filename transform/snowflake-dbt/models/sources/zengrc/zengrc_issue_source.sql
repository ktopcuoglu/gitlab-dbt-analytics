WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'issues') }}

),

renamed AS (

    SELECT
      code::VARCHAR              AS issue_code,
      created_at::TIMESTAMP      AS created_at,
      custom_attributes::VARIANT AS issue_custom_attributes,
      description::VARCHAR       AS issue_description,
      id::NUMBER                 AS issue_id,
      mapped__audits::VARIANT    AS mapped_audits,
      mapped__controls::VARIANT  AS mapped_controls,
      mapped__programs::VARIANT  AS mapped_programs,
      mapped__standards::VARIANT AS mapped_standards,
      notes::VARCHAR             AS issue_notes,
      status::VARCHAR            AS issue_status,
      stop_date::DATE            AS issue_stop_date,
      tags::VARCHAR              AS issue_tags,
      title::VARCHAR             AS issue_title,
      type::VARCHAR              AS zengrc_object_type,
      updated_at::TIMESTAMP      AS issue_updated_at,
      __loaded_at::TIMESTAMP     AS issue_loaded_at
    FROM source

)

SELECT *
FROM renamed