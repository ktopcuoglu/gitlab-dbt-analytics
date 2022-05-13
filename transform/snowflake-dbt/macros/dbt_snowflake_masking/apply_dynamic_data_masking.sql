{%- macro apply_dynamic_data_masking(columns) -%}

{% set materialization = 'view' if model.config.materialized == 'view' else 'table' %}
{% set database = generate_database_name(model.config.database) %}
{% set schema = model.config.schema %}
{% set alias = model.config.alias %}

{{ create_masking_policies(database, schema) }} 

{% for column in columns %}

    {% for column_name, column_type in column.items() %}

            alter {{materialization}} "{{database}}".{{schema}}.{{alias}} 
            modify column {{column_name}} 
            set masking policy "{{database}}".{{schema}}.hide_{{column_type}}_column_values;
        
    {% endfor %}

{% endfor %}

{%- endmacro -%}
