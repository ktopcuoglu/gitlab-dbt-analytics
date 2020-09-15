{% macro source_column_sum_min(schema, table, column, min_value, where_clause=None) %}

WITH source AS (

    SELECT *
    FROM {{ source(schema, table) }}

), counts AS (

    SELECT SUM({{column}}) AS sum_value
    FROM source
    {% if where_clause != None %}
    WHERE {{ where_clause }}
    {% endif %}

)

SELECT sum_value
FROM counts
WHERE sum_value < {{ min_value }}

{% endmacro %}
