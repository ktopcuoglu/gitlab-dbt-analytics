{%- macro create_masking_policy_hide_float_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_float_column_values AS (val float) 
  RETURNS float ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN 0
      ELSE val
      END;

{%- endmacro -%}
