{{ config({
    "unique_key": "run_unique_key"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'run') }}
    {% if is_incremental() %}
    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}

), flattened AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT          AS model_execution_time,
      data_by_row['node']['name']::VARCHAR          AS model_name,
      data_by_row['node']['schema']::VARCHAR        AS schema_name,
      data_by_row['node']['unique_id']::VARCHAR     AS unique_id,
      data_by_row['node']['tags']::ARRAY            AS model_tags,
      IFNULL(data_by_row['error']::VARCHAR, False)  AS model_error_text,
      IFNULL(data_by_row['fail']::VARCHAR, False)   AS model_fail,
      IFNULL(data_by_row['warn']::VARCHAR, False)   AS model_warn,
      data_by_row['skip']::BOOLEAN                  AS model_skip,
      uploaded_at,
      timing.value['started_at']::TIMESTAMP         AS compilation_started_at,
      timing.value['completed_at']::TIMESTAMP       AS compilation_completed_at,
      {{ dbt_utils.surrogate_key([
          'unique_id', 
          'compilation_started_at',
          'uploaded_at'
          ]) }}                                     AS run_unique_key
    FROM flattened
    LEFT JOIN LATERAL FLATTEN(INPUT => data_by_row['timing']::ARRAY, outer => true) timing
    ON IFNULL(timing.value['name'], 'compile') = 'compile'
  
)

SELECT *
FROM model_parsed_out
