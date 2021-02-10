    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_packages_packages_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                 AS packages_package_id,
      name::VARCHAR              AS package_name,
      project_id::NUMBER         AS project_id,
      creator_id::NUMBER         AS creator_id,
      version::VARCHAR           AS package_version,
      package_type::VARCHAR      AS package_type,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at
    FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
