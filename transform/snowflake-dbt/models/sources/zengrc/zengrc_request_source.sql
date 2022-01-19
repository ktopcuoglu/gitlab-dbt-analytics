WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'requests') }}

),

renamed AS (

    SELECT
      assignees::VARIANT         AS assignees,
      audit__id::NUMBER          AS audit_id,
      audit__title::VARCHAR      AS audit_title,
      code::VARCHAR              AS request_code,
      created_at::TIMESTAMP      AS request_created_at,
      custom_attributes::VARIANT AS request_custom_attributes,
      description::VARCHAR       AS request_description,
      end_date::DATE             AS request_end_date,
      id::NUMBER                 AS request_id,
      mapped__controls::VARIANT  AS mapped_controls,
      mapped__issues::VARIANT    AS mapped_issues,
      mapped__programs::VARIANT  AS mapped_programs,
      requesters::VARIANT        AS requestors,
      start_date::DATE           AS request_start_date,
      status::VARCHAR            AS request_status,
      stop_date::DATE            AS request_stop_date,
      tags::VARCHAR              AS request_tags,
      title::VARCHAR             AS request_title,
      type::VARCHAR              AS zengrc_object_type,
      updated_at::TIMESTAMP      AS request_updated_at,
      __loaded_at::TIMESTAMP     AS request_loaded_at
    FROM source

)

SELECT *
FROM renamed