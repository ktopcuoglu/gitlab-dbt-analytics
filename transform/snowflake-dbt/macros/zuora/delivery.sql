{%- macro delivery(product_category_column, output_column_name = 'delivery') -%}

CASE 
  WHEN LOWER({{product_category_column}}) LIKE '%saas%'
    THEN 'SaaS'
  WHEN LOWER({{product_category_column}}) LIKE '%self-managed%'
    THEN 'Self-Managed'
  WHEN {{product_category_column}} IN (
                                        'Basic'
                                      , 'GitHost'
                                      , 'Other'
                                      , 'Plus'
                                      , 'Standard'
                                      , 'Support'
                                      , 'Trueup'
                                      )
    THEN 'Others'
  ELSE NULL
END AS {{output_column_name}}

{%- endmacro -%}
