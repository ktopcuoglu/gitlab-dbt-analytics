{{ config({
    "materialized": "view"
    })
}}
WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'freshness') }}

), parsed AS (

    SELECT 
      REPLACE(REGEXP_REPLACE(s.path, '\\[|\\]|''', ''), 'source.gitlab_snowflake.', '')::VARCHAR    AS schema_table_name,
      SPLIT_PART(schema_table_name, '.', 1)                                                         AS schema_name,
      SPLIT_PART(schema_table_name, '.', -1)                                                        AS table_name,
      s.value['max_loaded_at']::TIMESTAMP                                                           AS latest_load_at,
      s.value['max_loaded_at_time_ago_in_s']::FLOAT                                                 AS time_since_loaded_seconds,
      s.value['state']::VARCHAR                                                                     AS source_freshness_state,
      s.value['snapshotted_at']::TIMESTAMP                                                          AS freshness_observed_at,
      {{ dbt_utils.surrogate_key(['schema_table_name', 'freshness_observed_at']) }}                 AS freshness_unique_key,
      uploaded_at
    FROM source 
    INNER JOIN LATERAL FLATTEN(jsontext['sources']) s
    WHERE s.value['state']::VARCHAR != 'runtime error'  -- impossible to know what freshness is, so filtered out

)
SELECT *
FROM parsed
