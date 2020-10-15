{% macro source_golden_data_comparison(source_name, table_name) %}

{% set golden_data_model = source_name + '_' + table_name + '_raw_golden_data' %}
{% set gr_columns = adapter.get_columns_in_relation(ref(golden_data_model)) %}
{% set gr_column_names = gr_columns|map(attribute='name')|list %}

WITH check_data AS (

    SELECT
      SUM(
      {%- for column in gr_column_names %}
          CASE WHEN golden_data.{{ column }} = source_table.{{ column }} THEN 0 ELSE 1 END
              {%- if not loop.last %}
                  +
              {% endif %}
      {% endfor %}
          ) AS is_incorrect
    FROM {{ ref(golden_data_model) }} golden_data
    LEFT JOIN {{ source(source_name, table_name) }} source_table ON
    {%- for column in gr_column_names %}
        source_table.{{ column }} = golden_data.{{ column }}
        {% if not loop.last %}
            AND
        {% endif %}
    {% endfor %}
)


SELECT *
FROM check_data
WHERE is_incorrect > 1

{% endmacro %}
