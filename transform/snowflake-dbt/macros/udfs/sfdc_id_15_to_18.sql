{% macro sfdc_id_15_to_18() %}

{%- set production_targets = production_targets() -%}
{%- set production_databases = ["ANALYTICS", "PREP", "PROD"] -%}

{% for db in production_databases %}
    {%- if target.name in production_targets -%}

    CREATE OR REPLACE FUNCTION {{ db | trim }}.{{target.schema}}.id15to18("input_id" string)

    {%- else -%}

    CREATE OR REPLACE FUNCTION "{{ target.database | trim }}_{{ db | trim }}".{{target.schema}}.id15to18("input_id" string)

    {% endif %}
    RETURNS string
    LANGUAGE JAVASCRIPT
    AS '
    let suffix = "";

    if (input_id.length != 15) {
        return input_id;
    }

    for (let index = 0; index < 3; index++) {
        let flags = 0;

        for (let inner_index = 0; inner_index < 5; inner_index++) {
            let chr = input_id.substr(index * 5 + inner_index,1)

            let ascii_code = chr.charCodeAt(0)

            if ((ascii_code >= "A".charCodeAt(0)) && (ascii_code <= "Z".charCodeAt(0))) {
                flags = flags + (1 << inner_index)
            }

        }
        suffix = suffix.concat("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345".substr(flags,1))

    }

    let final_id = input_id.concat(suffix)

    return final_id

    ';

    {% endfor %}
{% endmacro %}
