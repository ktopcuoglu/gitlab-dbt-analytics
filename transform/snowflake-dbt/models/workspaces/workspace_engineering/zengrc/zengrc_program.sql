{{ simple_cte([
    ('audits','zengrc_audit_source'),
    ('issues','zengrc_issue_source'),
    ('requests','zengrc_request_source')
])}}

, audit_programs AS (

    SELECT DISTINCT
      audits.program_id,
      audits.program_title,
      audits.program_type AS zengrc_object_type
    FROM audits
    WHERE audits.program_id IS NOT NULL

), issue_programs AS (

    SELECT DISTINCT
      mapped_programs.value['id']::NUMBER     AS program_id,
      mapped_programs.value['title']::VARCHAR AS program_title,
      mapped_programs.value['type']::VARCHAR  AS zengrc_type
    FROM issues
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(issues.mapped_programs)) mapped_programs

), requests_programs AS (

    SELECT DISTINCT
      mapped_programs.value['id']::NUMBER     AS program_id,
      mapped_programs.value['title']::VARCHAR AS program_title,
      mapped_programs.value['type']::VARCHAR  AS zengrc_type
    FROM requests
    INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(requests.mapped_programs)) mapped_programs

), unioned AS (

    SELECT *
    FROM audit_programs

    UNION 

    SELECT *
    FROM issue_programs

    UNION 

    SELECT *
    FROM requests_programs

)

SELECT *
FROM unioned

