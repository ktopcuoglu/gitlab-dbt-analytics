WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance_redis_metrics') }}

),
partiotioned AS (

    SELECT jsontext     AS jsontext,
           ping_date    AS ping_date,
           run_id       AS run_id,
           _uploaded_at AS _uploaded_at,
           MAX(run_id) OVER (PARTITION BY ping_date ORDER BY ping_date) AS run_id_max
      FROM base

),
renamed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['ping_date', 'run_id'])}} AS saas_usage_ping_redis_id,
      TRY_PARSE_JSON(jsontext) AS response,
      ping_date::TIMESTAMP     AS ping_date,
      run_id                   AS run_id,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS _uploaded_at
    FROM partiotioned
WHERE partiotioned.run_id = partiotioned.run_id_max

)

SELECT *
FROM renamed

