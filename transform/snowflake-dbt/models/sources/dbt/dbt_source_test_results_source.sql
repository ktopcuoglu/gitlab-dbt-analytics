{{ config({
    "unique_key": "test_unique_key"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'source_tests') }}
    {% if is_incremental() %}
    WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}

), flattened AS (

    SELECT 
      d.value                             AS data_by_row,
      jsontext['generated_at']::TIMESTAMP AS test_result_generated_at,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT                                      AS test_execution_time_elapsed,
      data_by_row['node']['unique_id']::VARCHAR                                 AS test_id,
      data_by_row['node']['name']::VARCHAR                                      AS test_name,
      data_by_row['error']::VARCHAR                                             AS test_error,
      IFNULL(data_by_row['node']['test_metadata']['name']::VARCHAR, 'custom')   AS test_type,
      data_by_row['node']['tags']::ARRAY                                        AS test_tags,
      ARRAY_CONTAINS('data'::VARIANT, test_tags)                                AS is_data_test,
      ARRAY_CONTAINS('schema'::VARIANT, test_tags)                              AS is_schema_test,
      data_by_row['node']['config']['severity']::VARCHAR                        AS test_severity,
      data_by_row['node']['depends_on']['nodes']::VARCHAR                       AS dependent_nodes,
      IFF(data_by_row['fail'] = 'true', True, False)                            AS is_failed_test,
      IFF(data_by_row['warn'] = 'true', True, False)                            AS is_warned_test,
      CASE
        WHEN is_failed_test = False AND is_warned_test = False
          THEN True
        ELSE False END                                                          AS is_passed_test,
      test_result_generated_at,
      {{ dbt_utils.surrogate_key(['test_id', 'test_result_generated_at']) }}    AS test_unique_key,
      uploaded_at
    FROM flattened
  
)

SELECT *
FROM model_parsed_out
