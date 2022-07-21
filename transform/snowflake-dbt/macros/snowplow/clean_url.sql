-- regex explanation available at https://regex101.com/r/FyIJzf/1

{% macro clean_url(column_name) -%}

RTRIM(REGEXP_REPLACE({{ column_name }}, '^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', ''), '/')

{%- endmacro %}