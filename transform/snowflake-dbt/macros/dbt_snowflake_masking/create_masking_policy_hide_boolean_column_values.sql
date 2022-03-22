{%- macro create_masking_policy_hide_boolean_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_boolean_column_values AS (val boolean) 
  RETURNS boolean ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END;

{%- endmacro -%}
