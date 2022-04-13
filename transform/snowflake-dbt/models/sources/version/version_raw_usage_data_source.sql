{{ config({
    "materialized": "incremental",
    "unique_key": "raw_usage_data_id"
    })
}}

WITH data AS (
  SELECT *
  FROM {{ source('version', 'raw_usage_data') }}
    WHERE created_at >= '2022-01-01' and created_at < '2022-02-01'

), source AS (

    SELECT *
    FROM data
    {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY recorded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER            AS raw_usage_data_id,
      PARSE_JSON(payload)    AS raw_usage_data_payload,
      created_at::TIMESTAMP  AS created_at,
      recorded_at::TIMESTAMP AS recorded_at
    FROM source

)

SELECT *
FROM renamed
