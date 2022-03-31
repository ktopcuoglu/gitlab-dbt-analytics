{%- macro create_masking_policies(database, schema) -%}

{{ create_masking_policy_hide_float_column_values(database, schema) }}
{{ create_masking_policy_hide_string_column_values(database, schema) }}
{{ create_masking_policy_hide_array_column_values(database, schema) }}
{{ create_masking_policy_hide_variant_column_values(database, schema) }}
{{ create_masking_policy_hide_boolean_column_values(database, schema) }}
{{ create_masking_policy_hide_number_column_values(database, schema) }}
{{ create_masking_policy_hide_date_column_values(database, schema) }}

{%- endmacro -%}
