WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'shards') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER                      AS shard_id,
    name::VARCHAR                   AS shard_name
  FROM source

)


SELECT *
FROM renamed