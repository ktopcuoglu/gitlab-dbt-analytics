{%- macro create_masking_policy_hide_number_column_values(database, schema) -%}

CREATE MASKING POLICY IF NOT EXISTS "{{database}}".{{schema}}.hide_number_column_values AS (val number(38,0)) 
  RETURNS number(38,0) ->
      CASE WHEN CURRENT_ROLE() IN ('DATA_OBSERVABILITY') THEN 0
      ELSE val
      END;

{%- endmacro -%}
