{%- macro convert_variant_to_boolean_field(value) -%}

  UPPER(TRY_TO_BOOLEAN({{ value }}::VARCHAR))

{%- endmacro -%}


