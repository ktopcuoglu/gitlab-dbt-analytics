{%- macro apply_dynamic_data_masking(database, schema, table) -%}

{# Create all masking policies needed by type of PII column we need to mask #}
{# {{ create_masking_policies(database, schema) }} #} 

{# 
{% set column_name = 'arr' %}
alter table {{database}}.{{schema}}.{{table}} 
modify column {{column_name}} 
set masking policy {{database}}.{{schema}}.hide_float_column_values;
#}

{# 
  # Loop through the columns of the model, check if is_pii is true and only for those run the alter statement 
  # Based on the data type of the pii column, run the appropriate masking policy (variant, boolean, string etc.)

  # Choosing option to pass a column_list. 

{% for column in column_list %}
    alter table {{database}}.{{schema}}.{{table}} 
    modify column {{column}} 
    set masking policy {{database}}.{{schema}}.hide_float_column_values;
{% endfor %}
#}

{% for column_name, column_properties in model.get('columns').items() %}

    {% if column_properties.get('meta').get('contains_pii') or  column_properties.get('tags').get('pii')  %}
        alter table {{database}}.{{schema}}.{{table}} 
        modify column {{column}} 
        set masking policy {{database}}.{{schema}}.hide_float_column_values;
    {% endif %}

{% endfor %}

{%- endmacro -%}
