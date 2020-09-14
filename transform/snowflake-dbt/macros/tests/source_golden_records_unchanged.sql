{% macro source_golden_records_unchanged(source_model) %}

{% set golden_record_model = source_model + '_golden_records' %}

{% set meta_columns = get_meta_columns(golden_record_model, "sensitive") %}

WITH check_data AS (

    SELECT

    SUM(
    {%- for column in meta_columns %}
        CASE WHEN IFNULL(golden_records.{{ column }}, '') = IFNULL(golden_records.{{ column }}, '') THEN 0 ELSE 1 END
            {%- if not loop.last %}
                +
            {% endif %}
        {% endfor %}
        ) AS is_correct
    FROM {{ ref(golden_record_model) }} golden_records
    JOIN {{ ref(source_model) }} ON
    {%- for column in meta_columns %}
        source_model.{{ column }} = golden_records.{{ column  }} AND
    {% endfor %}
)
    SELECT *
    FROM check_data
    WHERE is_correct > 1
{% endmacro %}
