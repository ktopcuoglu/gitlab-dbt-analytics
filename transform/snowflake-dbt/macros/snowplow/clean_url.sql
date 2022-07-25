{% macro clean_url(column_name) -%}

RTRIM(REGEXP_REPLACE({{ column_name }}, '[0-9]{4}/[0-9]{2}/[0-9]{2}/|^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', ''), '/')

{%- endmacro %}