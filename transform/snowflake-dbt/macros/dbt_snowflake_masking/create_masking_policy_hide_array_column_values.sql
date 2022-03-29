{%- macro create_masking_policy_hide_array_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_array_column_values AS (val array) 
  RETURNS array ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN NULL
      ELSE val
      END; 

{%- endmacro -%}
