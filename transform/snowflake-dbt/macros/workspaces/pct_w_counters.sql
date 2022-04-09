{% macro pct_w_counters(active_count, inactive_count) -%}

DIV0({{ active_count }},({{ active_count }}+{{ inactive_count }}))

{%- endmacro %}
