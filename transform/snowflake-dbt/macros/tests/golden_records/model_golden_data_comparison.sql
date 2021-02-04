{% macro model_golden_data_comparison(dbt_model) %}

{% set golden_data_model = dbt_model + '_golden_data' %}
{% set gr_columns = adapter.get_columns_in_relation(ref(golden_data_model)) %}
{% set gr_column_names = gr_columns|map(attribute='name')|list %}

WITH check_data AS (

    SELECT
      SUM(
      {%- for column in gr_column_names %}
          CASE WHEN golden_data.{{ column }}::VARCHAR = dbt_model.{{ column }}::VARCHAR THEN 0 ELSE 1 END
              {%- if not loop.last %}
                  +
              {% endif %}
          {% endfor %}
          ) AS is_incorrect
    FROM {{ ref(golden_data_model) }} golden_data
    LEFT JOIN {{ ref(dbt_model) }} dbt_model ON
    {%- for column in gr_column_names %}
        dbt_model.{{ column }}::VARCHAR = golden_data.{{ column }}::VARCHAR
        {% if not loop.last %}
            AND
        {% endif %}
    {% endfor %}
)


SELECT *
FROM check_data
WHERE is_incorrect > 1

{% endmacro %}
