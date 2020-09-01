{%- macro type_of_arr_change(column_1, column_2) -%}

--column_1 = arr, column_2 = previous_arr

    CASE
      WHEN {{ column_2 }} = 0 AND {{ column_1 }} > 0 THEN 'New'
      WHEN {{ column_1 }} = 0 AND {{ column_2 }} > 0 THEN 'Churn'
      WHEN {{ column_1 }} < {{ column_2 }} AND {{ column_1 }} > 0 THEN 'Contraction'
      WHEN {{ column_1 }} > {{ column_2 }} THEN 'Expansion'
      WHEN {{ column_1 }} = {{ column_2 }} THEN 'No Impact'
      ELSE NULL
    END                 AS type_of_arr_change

{%- endmacro -%}
