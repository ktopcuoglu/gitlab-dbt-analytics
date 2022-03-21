{%- macro create_masking_policy_hide_variant_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_variant_column_values AS (val variant) 
  RETURNS variant ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END;

{%- endmacro -%}
