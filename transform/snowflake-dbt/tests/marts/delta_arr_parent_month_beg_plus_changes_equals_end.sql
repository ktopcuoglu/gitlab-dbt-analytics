--Test that the beg ARR, plus the changes that are calculated independently, equals the ending ARR. Also check to see that
--the end arr ties out to the arr in arr_data_mart.
{{ config({
    "tags": ["tdf","mart","arr"]
    })
}}

WITH test AS (

    SELECT
      arr_month,
      ultimate_parent_account_id,
      SUM(beg_arr + seat_change_arr + price_change_arr + tier_change_arr)       AS beg_plus_changes_arr,
      SUM(end_arr)                                                              AS end_arr,
      SUM(beg_quantity + seat_change_quantity)                                  AS beg_plus_changes_quantity,
      SUM(end_quantity)                                                         AS end_quantity
    FROM {{ ref('mart_delta_arr_parent_month') }}
    GROUP BY 1,2

), arr_data_mart AS(

    SELECT
      arr_month,
      ultimate_parent_account_id,
      SUM(mrr*12)        AS arr,
      SUM(quantity)      AS quantity
    FROM {{ ref('arr_data_mart') }}
    GROUP BY 1,2

), test_two AS (

    SELECT
      arr_month,
      ultimate_parent_account_id,
      SUM(beg_arr) AS beg_arr,
      SUM(end_arr) AS end_arr
    FROM {{ ref('mart_delta_arr_parent_month') }}
    GROUP BY 1,2
    ORDER BY 1 DESC

), variance AS (

    SELECT
      test.arr_month,
      test.ultimate_parent_account_id,
      ROUND(test.beg_plus_changes_arr - test.end_arr)                                        AS arr_variance,
      test.beg_plus_changes_quantity - test.end_quantity                                     AS quanity_variance,
      ROUND(test.end_arr - arr_data_mart.arr)                                                AS arr_data_mart_arr_variance,
      test.end_quantity - arr_data_mart.quantity                                             AS arr_data_mart_quantity_variance,
      CASE
        WHEN ROUND(test_two.beg_arr - LAG(test_two.end_arr) OVER (PARTITION BY test_two.ultimate_parent_account_id ORDER BY test_two.arr_month)) IS NULL
        THEN 0
        ELSE ROUND(test_two.beg_arr - LAG(test_two.end_arr) OVER (PARTITION BY test_two.ultimate_parent_account_id ORDER BY test_two.arr_month))
      END                                                                                    AS beg_end_variance
    FROM test
    LEFT JOIN arr_data_mart
      ON test.arr_month = arr_data_mart.arr_month
      AND test.ultimate_parent_account_id = arr_data_mart.ultimate_parent_account_id
    LEFT JOIN test_two
      ON test.arr_month = test_two.arr_month
      AND test.ultimate_parent_account_id = test_two.ultimate_parent_account_id

)

SELECT *
FROM variance
WHERE arr_variance != 0
  OR quanity_variance != 0
  OR arr_data_mart_arr_variance != 0
  OR arr_data_mart_quantity_variance != 0
  OR beg_end_variance != 0
