WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance_redis_metrics') }}

), renamed AS (

    SELECT
      TRY_PARSE_JSON(jsontext)   AS response
    FROM base  

)

SELECT *

FROM renamed

