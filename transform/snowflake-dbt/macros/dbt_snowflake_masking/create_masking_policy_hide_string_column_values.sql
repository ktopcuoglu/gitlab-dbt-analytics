{%- macro create_masking_policy_hide_string_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_string_column_values AS (val string) 
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN '**********'
      ELSE val
      END;

{%- endmacro -%}
