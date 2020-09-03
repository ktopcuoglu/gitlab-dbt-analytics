{% macro source_golden_records_unchanged(golden_record_model, hashed_model, join_column) %}


WITH sheetload_data AS (

    SELECT

        {{ hash_sensitive_columns(golden_record_model)}}
    FROM {{ ref(golden_record_model) }}

), hashed_data AS (

    SELECT
        *
    FROM {{ ref(hashed_model) }}

),
check_data AS (

{% set meta_columns = get_meta_columns(golden_record_model, "sensitive") %}


    SELECT
        sheetload.account_id,
         SUM(
        {%- for column in meta_columns %}
        CASE WHEN
            sheetload.{{ column }}_hash = hashed.{{ column }}_hash THEN 0 ELSE 1
        END +
        {% endfor %}
        -- Terminate the last +
        0 )
        AS num_rows
    FROM sheetload_data sheetload
    LEFT JOIN hashed_data hashed ON hashed.{{ join_column }} = sheetload.{{ join_column }}
    WHERE sheetload.{{ column}}_hash IS NOT NULL
    GROUP BY sheetload.account_id
)

    SELECT *
    FROM check_data
    WHERE num_rows > 0

{% endmacro %}
