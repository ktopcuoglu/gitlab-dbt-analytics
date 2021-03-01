WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_licenses_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                 AS license_id,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at

    FROM source

)

SELECT *
FROM renamed
