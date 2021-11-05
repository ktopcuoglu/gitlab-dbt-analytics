WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance_sql_errors') }}

), partitioned AS (

    SELECT run_id       AS run_id,
           sql_errors   AS sql_errors,
           ping_date    AS ping_date,
           _uploaded_at AS _uploaded_at
      FROM base
      QUALIFY ROW_NUMBER() OVER (PARTITION BY ping_date ORDER BY ping_date DESC) = 1

), renamed AS (

    SELECT
      run_id                                              AS run_id,
      TRY_PARSE_JSON(sql_errors)                          AS sql_errors,
      ping_date::TIMESTAMP                                AS ping_date,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS _uploaded_at
    FROM partitioned

)

SELECT *
FROM renamed

