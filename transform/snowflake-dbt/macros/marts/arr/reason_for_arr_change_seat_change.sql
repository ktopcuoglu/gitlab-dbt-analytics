{%- macro reason_for_arr_change_seat_change(column_1, column_2, column_3, column_4) -%}

--column_1 = arr, column_2 = previous_arr, column_3 = quantity, column_4 = previous_quantity

    CASE
      WHEN {{ column_4 }} != {{ column_3 }} AND {{ column_4 }} > 0
        THEN ZEROIFNULL({{ column_2 }}/NULLIF({{ column_4 }},0) * ({{ column_3 }} - {{ column_4 }}))
      WHEN {{ column_4 }} != {{ column_3 }} AND {{ column_4 }} = 0
        THEN {{ column_1 }}
      ELSE 0
    END                AS seat_change_arr

{%- endmacro -%}
