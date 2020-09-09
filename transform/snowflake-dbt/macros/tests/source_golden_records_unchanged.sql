{% macro source_golden_records_unchanged(golden_record_model, source_model, join_column, check_column) %}


WITH check_data AS (

    SELECT
        CASE WHEN IFNULL(golden_records.{{ check_column }}, '') = IFNULL(golden_records.{{ check_column }}, '')
        THEN 0 ELSE 1 END AS num_rows
    FROM {{ ref(golden_record_model) }} golden_records
    LEFT JOIN {{ ref(source_model) }} ON source_model.{{ join_column }} = golden_records.{{ join_column }}

)
    SELECT *
    FROM check_data
    WHERE num_rows > 0

{% endmacro %}
