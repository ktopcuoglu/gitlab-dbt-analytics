WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance_redis_metrics') }}

), renamed AS (

    SELECT
      TRY_PARSE_JSON(jsontext) AS response,
      ping_date::TIMESTAMP     AS ping_date,
      run_id                   AS run_id,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS _uploaded_at
    FROM base

)

SELECT *
FROM renamed

