{% macro usage_estimation(active_metric_count, percent_active) -%}

{{ active_metric_count }} + DIV0(({{ active_metric_count }} * (1 - {{ percent_active }} )),{{ percent_active }} )

{%- endmacro %}
