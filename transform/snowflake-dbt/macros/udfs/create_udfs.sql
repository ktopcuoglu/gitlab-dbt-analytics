{% macro create_udfs() %}

{%- set production_targets = production_targets() -%}

{%- if target.name in production_targets -%}

  create schema if not exists {{target.schema}};

    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}

{%- else -%}
    
    create schema if not exists "{{ target.database | trim }}_ANALYTICS".{{target.schema}};

    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}

{%- endif -%}


{% endmacro %}
