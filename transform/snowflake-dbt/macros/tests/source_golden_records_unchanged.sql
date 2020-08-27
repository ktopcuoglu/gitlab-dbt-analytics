{% macro source_golden_records_unchanged(sheetload_model, hashed_model, join_column) %}

WITH sheetload_data AS (

    SELECT
        *
    FROM {{ ref(sheetload_model) }}

), hashed_data AS (

    SELECT
        *
    FROM {{ ref(hashed_model) }}

)

SELECT COUNT(*)
FROM sheetload_data sheetload
JOIN hashed_data hashed ON hashed.{{ join_column }} = sheetload.{{ join_column }}

{% endmacro %}
