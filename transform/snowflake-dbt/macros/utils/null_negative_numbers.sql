{%- macro null_negative_numbers(value) -%}

  IFF( TRY_TO_NUMBER({{ value }}) < 0, NULL, TRY_TO_NUMBER({{ value }}))

{%- endmacro -%}

