WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_shards_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER                      AS shard_id,
    name::VARCHAR                   AS shard_name
  FROM source

)


SELECT *
FROM renamed
