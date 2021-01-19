WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'shards') }}

), renamed AS (

  SELECT
    id::NUMBER                      AS shard_id,
    name::VARCHAR                   AS shard_name
  FROM source

)


SELECT *
FROM renamed