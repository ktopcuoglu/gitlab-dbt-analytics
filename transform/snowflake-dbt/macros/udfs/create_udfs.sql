{% macro create_udfs() %}

{%- set production_targets = production_targets() -%}

{%- if target.name in production_targets -%}

  create schema if not exists {{target.schema}};

    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}

{%- else -%}
    
    {# Need to create analytics for gitlab_dotcom models #}
    create schema if not exists "{{ target.database | trim }}_ANALYTICS".{{target.schema}};
    create schema if not exists "{{ target.database | trim }}_PREP".{{target.schema}};

    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}

{%- endif -%}


{% endmacro %}
