WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'assessments') }}

),

renamed AS (

    SELECT
      assessors::VARIANT      AS assessors,
      code::VARCHAR           AS assessment_code,
      conclusion::VARCHAR     AS assessment_conclusion,
      control__id::NUMBER     AS control_id,
      control__title::VARCHAR AS control_title,
      control__type::VARCHAR  AS control_type,
      created_at::TIMESTAMP   AS assessment_created_at,
      description::VARCHAR    AS assessment_description,
      end_date::DATE          AS assessment_end_date,
      id::NUMBER              AS assessment_id,
      mapped__audits::VARIANT AS mapped_audits,
      start_date::DATE        AS assessment_start_date,
      status::VARCHAR         AS assessment_status,
      title::VARCHAR          AS assessment_title,
      type::VARCHAR           AS zengrc_object_type,
      updated_at::TIMESTAMP   AS assessment_uploaded_at,
      __loaded_at::TIMESTAMP  AS assessment_loaded_at
    FROM source

)

SELECT *
FROM renamed



