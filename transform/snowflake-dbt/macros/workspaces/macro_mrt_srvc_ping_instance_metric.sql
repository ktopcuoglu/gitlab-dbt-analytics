{% macro macro_mrt_srvc_ping_instance_metric(model_name1) %}


    SELECT TOP 10 * FROM {{ ref(model_name1) }}
{% endmacro %}
