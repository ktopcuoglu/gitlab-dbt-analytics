-- regex explanation available at https://regex101.com/r/nSdGr7/1

{% macro clean_url(column_name) -%}

RTRIM(REGEXP_REPLACE({{ column_name }}, '\d{4}\/\d{2}\/\d{2}\/|^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', ''), '/')

{%- endmacro %}