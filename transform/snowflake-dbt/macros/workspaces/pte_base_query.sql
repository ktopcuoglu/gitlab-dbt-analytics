{%- macro pte_base_query(model_run_type) -%}

{%- if model_run_type=='training' -%}
{% set period_type = 'MONTH'%}
{% set delta_value = 3 %}
{% endif %}
{% if model_run_type=='scoring'  %}
{% set period_type = 'MONTH'%}
{% set delta_value = 10 %}
{% endif %}
SELECT '{{ delta_value }}' AS delta_value
{%- endmacro -%}
