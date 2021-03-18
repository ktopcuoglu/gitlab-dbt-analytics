WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance') }}

), renamed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['ping_date', 'run_results'])}} AS saas_usage_ping_gitlab_dotcom_id,
      TRY_PARSE_JSON(query_map)  AS query_map,
      TRY_PARSE_JSON(run_results) AS run_results,
      ping_date::TIMESTAMP        AS ping_date,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP     AS _uploaded_at
    FROM base  

)

SELECT *
FROM renamed
