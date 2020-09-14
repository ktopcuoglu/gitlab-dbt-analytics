{% macro source_golden_records_unchanged(source_model) %}

{% set golden_record_model = source_model + '_golden_records' %}

set (golden_record_count) = (
    SELECT COUNT(*)
    FROM {{ ref(golden_record_model) }}
)

set (joined_count) = (

    SELECT
      COUNT(*)
    FROM {{ ref(golden_record_model) }} golden_records
    NATURAL JOIN {{ ref(source_model) }}
)

SELECT IFF($joined_count = $golden_record_count, NULL, 1)

{% endmacro %}
