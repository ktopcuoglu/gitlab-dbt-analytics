{%- macro number_of_seats_buckets(number_of_seats) -%}

    CASE {{ number_of_seats }}
      WHEN BETWEEN 1 AND 5 THEN '[01] 1-5'
      WHEN BETWEEN 6 AND 7 THEN '[02] 6-7'
      WHEN BETWEEN 8 AND 10 THEN '[03] 8-10'
      WHEN BETWEEN 11 AND 15 THEN '[04] 11-15'
      WHEN BETWEEN 16 AND 50 THEN '[05] 16-50'
      WHEN BETWEEN 51 AND 100 THEN '[06] 51-100'
      WHEN BETWEEN 101 AND 200 THEN '[07] 101-200'
      WHEN BETWEEN 201 AND 500 THEN '[08] 201-500'
      WHEN BETWEEN 501 AND 1000 THEN '[09] 501-1,000'
      WHEN >= 1001 THEN '[10] 1,001+'
    END

{%- endmacro -%}
