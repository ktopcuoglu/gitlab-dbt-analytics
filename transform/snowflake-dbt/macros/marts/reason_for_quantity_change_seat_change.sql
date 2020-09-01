{%- macro reason_for_arr_change_seat_change(column_1, column_2, column_3, column_4) -%}

--column_1 = quantity, column_2 = previous_quantity

    CASE
      WHEN {{ column_2 }} != {{ column_1 }}
      THEN {{ column_1 }} - {{ column_2 }}
      ELSE 0
    END                AS seat_change_quantity

{%- endmacro -%}
