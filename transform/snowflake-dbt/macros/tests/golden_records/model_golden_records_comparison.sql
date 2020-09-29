{% macro model_golden_records_comparison(dbt_model) %}

{% set golden_record_model = dbt_model + '_golden_record' %}
{% set gr_columns = adapter.get_columns_in_relation(ref(golden_record_model)) %}
{% set gr_column_names = gr_columns|map(attribute='name')|list %}

WITH check_data AS (

    SELECT
      SUM(
      {%- for column in gr_column_names %}
          CASE WHEN golden_records.{{ column }} = dbt_model.{{ column }} THEN 0 ELSE 1 END
              {%- if not loop.last %}
                  +
              {% endif %}
          {% endfor %}
          ) AS is_incorrect
    FROM {{ ref(golden_record_model) }} golden_records
    LEFT JOIN {{ ref(dbt_model) }} dbt_model ON
    {%- for column in gr_column_names %}
        dbt_model.{{ column }} = golden_records.{{ column }}
        {% if not loop.last %}
            AND
        {% endif %}
    {% endfor %}
)


SELECT *
FROM check_data
WHERE is_incorrect > 1

{% endmacro %}
