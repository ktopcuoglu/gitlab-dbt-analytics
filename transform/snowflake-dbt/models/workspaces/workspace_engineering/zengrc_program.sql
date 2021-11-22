WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_audit_source') }}

)

SELECT DISTINCT
  source.program_id,
  source.program_title,
  source.program_type AS zengrc_object_type
FROM source
WHERE source.program_id IS NOT NULL
