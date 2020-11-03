{%- macro arr_buckets(arr) -%}

    CASE
      WHEN {{ arr }} < 0                        THEN '[00] < 0'
      WHEN {{ arr }} BETWEEN 0 AND 250          THEN '[01] 0-250'
      WHEN {{ arr }} BETWEEN 250 AND 500        THEN '[02] 250-500'
      WHEN {{ arr }} BETWEEN 500 AND 1000       THEN '[03] 500-1K'
      WHEN {{ arr }} BETWEEN 1000 AND 2500      THEN '[04] 1K-2.5K'
      WHEN {{ arr }} BETWEEN 2500 AND 5000      THEN '[05] 2.5K-5K'
      WHEN {{ arr }} BETWEEN 5000 AND 10000     THEN '[06] 5K-10K'
      WHEN {{ arr }} BETWEEN 10000 AND 25000    THEN '[07] 10K-25K'
      WHEN {{ arr }} BETWEEN 25000 AND 50000    THEN '[08] 25K-50K'
      WHEN {{ arr }} BETWEEN 50000 AND 100000   THEN '[09] 50K-100K'
      WHEN {{ arr }} BETWEEN 100000 AND 500000  THEN '[10] 100K-500K'
      WHEN {{ arr }} BETWEEN 500000 AND 1000000 THEN '[11] 500K-1M'
      WHEN {{ arr }} > 1000000                  THEN '[12] 1M+'
    END

{%- endmacro -%}
