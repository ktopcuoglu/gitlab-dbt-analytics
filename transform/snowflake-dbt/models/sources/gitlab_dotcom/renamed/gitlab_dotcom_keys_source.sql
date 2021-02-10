    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_keys_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER             AS key_id,
      user_id::NUMBER        AS user_id,
      created_at::TIMESTAMP   AS created_at,
      updated_at::TIMESTAMP   AS updated_at,
      type::VARCHAR           AS key_type,
      public::BOOLEAN         AS is_public,
      last_used_at::TIMESTAMP AS last_updated_at

    FROM source

)

SELECT *
FROM renamed
