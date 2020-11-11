{% macro regexp_substr_to_array() %}

{%- set production_targets = production_targets() -%}

{%- if target.name in production_targets -%}

CREATE OR REPLACE FUNCTION {{target.schema}}.regexp_to_array("input_text" string,
                                                             "regex_text" STRING)

{%- else -%}

CREATE OR REPLACE FUNCTION "{{ target.database | trim }}_ANALYTICS".{{target.schema}}.regexp_to_array("input_text" string,
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
{% endmacro %}
