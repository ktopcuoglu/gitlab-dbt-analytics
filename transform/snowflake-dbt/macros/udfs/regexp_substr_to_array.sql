{% macro regexp_substr_to_array() %}

{%- set production_targets = production_targets() -%}
{%- set production_databases = ["ANALYTICS", "PREP", "PROD"] -%}

{% for db in production_databases %}
    {%- if target.name in production_targets -%}

    CREATE OR REPLACE FUNCTION {{ db | trim }}.{{target.schema}}.regexp_to_array("input_text" string,
                                                                "regex_text" STRING)

    {%- else -%}

    CREATE OR REPLACE FUNCTION "{{ target.database | trim }}_{{ db | trim }}".{{target.schema}}.regexp_to_array("input_text" string,
                                                                        "regex_text" STRING)

    {% endif %}
    RETURNS array
    LANGUAGE JAVASCRIPT
    AS '

    var regex_constructor = new RegExp(regex_text, "g")
    matched_substr_array = input_text.match(regex_constructor);
    if (matched_substr_array == null){
        matched_substr_array = []
    }
    return matched_substr_array

';

{% endfor %}
{% endmacro %}
