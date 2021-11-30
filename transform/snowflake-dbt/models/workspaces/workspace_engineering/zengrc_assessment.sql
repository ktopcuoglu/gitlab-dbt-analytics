WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_assessment_source') }}

)

SELECT
  assessment_code,
  assessment_conclusion,
  control_id,
  assessment_created_at,
  assessment_description,
  assessment_end_date,
  assessment_id,
  assessment_start_date,
  assessment_status,
  assessment_title,
  zengrc_object_type,
  assessment_uploaded_at,
  assessment_loaded_at
FROM source