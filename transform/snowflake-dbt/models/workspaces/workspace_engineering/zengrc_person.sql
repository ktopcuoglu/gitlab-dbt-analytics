WITH audits AS (
  SELECT
    *
  FROM {{ ref('zengrc_audit_source') }}
),

  assessments AS (
    SELECT
      *
    FROM {{ ref('zengrc_assessment_source') }}
  ),

  audit_managers AS (
    SELECT DISTINCT
      audut_manager.value['id']::NUMBER    AS person_id,
      audut_manager.value['name']::VARCHAR AS person_name,
      audut_manager.value['type']::VARCHAR AS zengrc_type
    FROM audits
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(audits.audit_managers)) audut_manager
  ),

  assessors AS (
    SELECT DISTINCT
      assessors.value['id']::NUMBER    AS person_id,
      assessors.value['name']::VARCHAR AS person_name,
      assessors.value['type']::VARCHAR AS zengrc_type
    FROM assessments
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(assessments.assessors)) assessors
  ),

  unioned AS (
    SELECT
      *
    FROM audit_managers
    UNION
    SELECT
      *
    FROM assessors
  )
SELECT
  *
FROM unioned