{% macro model_column_existence(model_name, column_list) %}

{% set error_count = namespace(value=0) %}
{% set meta_columns = get_meta_columns(golden_record_model) %}

{%- for column in column_list -%}
        {% if column not in meta_columns %}
              {% set count.value = count.value + 1 %}
        {% endif %}
{%- endfor -%}

{% endmacro %}
