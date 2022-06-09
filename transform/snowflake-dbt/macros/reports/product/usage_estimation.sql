{% macro usage_estimation(reporting_metric_count, percent_reporting) -%}

{{ reporting_metric_count }} + DIV0(({{ reporting_metric_count }} * (1 - {{ percent_reporting }} )),{{ percent_reporting }} )

{%- endmacro %}
