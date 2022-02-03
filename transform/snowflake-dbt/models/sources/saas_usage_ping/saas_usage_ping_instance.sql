WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance') }}

), partitioned AS (

    SELECT
      query_map                AS query_map,
      run_results              AS run_results,
      ping_date                AS ping_date,
      run_id                   AS run_id,
      recorded_at              AS recorded_at,
      version                  AS version,
      edition                  AS edition,
      recording_ce_finished_at AS recording_ce_finished_at,
      recording_ee_finished_at AS recording_ee_finished_at,
      uuid                     AS uuid,
      _uploaded_at             AS _uploaded_at
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ping_date ORDER BY ping_date DESC) = 1

), renamed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['ping_date', 'run_results'])}} AS saas_usage_ping_gitlab_dotcom_id,
      TRY_PARSE_JSON(query_map)                                  AS query_map,
      TRY_PARSE_JSON(run_results)                                AS run_results,
      ping_date::TIMESTAMP                                       AS ping_date,
      run_id                                                     AS run_id,
      recorded_at::TIMESTAMP                                     AS recorded_at,
      version                                                    AS version,
      edition                                                    AS edition,
      recording_ce_finished_at::TIMESTAMP                        AS recording_ce_finished_at,
      recording_ee_finished_at::TIMESTAMP                        AS recording_ee_finished_at,
      uuid                                                       AS uuid,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP        AS _uploaded_at
    FROM partitioned

)

SELECT *
FROM renamed
