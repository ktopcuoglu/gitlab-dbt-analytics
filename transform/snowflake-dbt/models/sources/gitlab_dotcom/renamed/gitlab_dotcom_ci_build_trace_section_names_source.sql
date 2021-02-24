WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_build_trace_section_names_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER           AS ci_build_id, 
      project_id::NUMBER   AS project_id,
      name::VARCHAR         AS ci_build_name

    FROM source

)


SELECT *
FROM renamed
