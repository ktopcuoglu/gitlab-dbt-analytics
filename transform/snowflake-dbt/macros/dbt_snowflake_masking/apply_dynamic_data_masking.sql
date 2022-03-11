{%- macro apply_dynamic_data_masking(database, columns) -%}

{% set materialization = 'view' if model.config.materialized == 'view' else 'table' %}
{# {% set database = model.config.database %} #}
{% set schema = model.config.schema %}
{% set alias = model.config.alias %}

{{ create_masking_policies(database, schema) }} 

{% for column in columns %}

    {% for column_name, column_type in column.items() %}

        {% if column_type == "float" %}

            alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_float_column_values;

        {% elif column_type == "array" %}

            alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_array_column_values;

        {% elif column_type == "boolean" %}

            alter {{materialization}} {{database}}.{{schema}}.{{model}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_boolean_column_values;

        {% elif column_type == "number" %}

            alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_number_column_values;

        {% elif column_type == "string" %}

            alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_string_column_values;
       
        {% elif column_type == "variant" %}

            alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy {{database}}.{{schema}}.hide_variant_column_values;
        
        {% endif %}

    {% endfor %}

{% endfor %}

{%- endmacro -%}
