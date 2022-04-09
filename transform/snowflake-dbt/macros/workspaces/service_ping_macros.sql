{% macro usage_estimation(active_metric_count, percent_active) -%}

{{ active_metric_count }} + DIV0(({{ active_metric_count }} * (1 - {{ percent_active }} )),{{ percent_active }} )

{%- endmacro %}

{% macro pct_w_counters(active_count, inactive_count) -%}

DIV0({{ active_count }},({{ active_count }}+{{ inactive_count }}))

{%- endmacro %}
