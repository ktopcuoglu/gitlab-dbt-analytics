WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'assessments') }}

),

renamed AS (

    SELECT
        code::VARCHAR          AS assessment_code,
        conclusion::VARCHAR    AS assessment_conclusion,
        created_at::TIMESTAMP  AS assessment_created_at,
        description::VARCHAR   AS assessment_description,
        end_date::DATE         AS assessment_end_date,
        id::NUMBER             AS assessment_id,
        start_date::DATE       AS assessment_start_date,
        status::VARCHAR        AS assessment_status,
        title::VARCHAR         AS assessment_title,
        type::VARCHAR          AS zengrc_object_type,
        updated_at::TIMESTAMP  AS assessment_uploaded_at,
        __loaded_at::TIMESTAMP AS assessment_laoded_at
    FROM source

)

SELECT *
FROM renamed



