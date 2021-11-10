WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_dast_profiles_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                        AS dast_profiles_id,
    project_id::NUMBER                AS project_id,
    dast_site_profile_id::NUMBER      AS dast_site_profile_id,
    dast_scanner_profile_id::NUMBER   AS dast_scanner_profile_id,
    created_at::TIMESTAMP             AS created_at, 
    updated_at::TIMESTAMP             AS updated_at,
    name::VARCHAR                     AS name,
    description::VARCHAR              AS description,
    branch_name::VARCHAR              AS branch_name
  FROM source

)

SELECT *
FROM renamed
ORDER BY created_at