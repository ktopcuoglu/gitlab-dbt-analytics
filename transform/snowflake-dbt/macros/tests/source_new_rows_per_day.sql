{% macro source_new_rows_per_day(schema, table, created_column, min_value, max_value, where_clause=None) %}

WITH dates as (

    SELECT *
    FROM {{ ref('date_details' )}}
    WHERE is_holiday = FALSE
    AND day_of_week IN (2,3,4,5,6)

), source as (

    SELECT *
    FROM {{ source(schema, table) }}

), counts AS (

    SELECT 
      count(*)                             AS row_count,
      date_trunc('day',{{created_column}}) AS the_day
    FROM source
    WHERE the_day IN (SELECT DATE_ACTUAL FROM dates)
    {% if where_clause != None %}
      AND {{ where_clause }}
    {% endif %}
    GROUP BY 2
    LIMIT 1

)

SELECT row_count
FROM counts
WHERE row_count < {{ min_value }} OR row_count > {{ max_value }}

{% endmacro %}
