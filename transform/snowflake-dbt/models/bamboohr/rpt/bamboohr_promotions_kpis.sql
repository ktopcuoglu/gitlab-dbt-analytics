WITH dates AS (

    SELECT date_actual AS month_date
    FROM {{ ref('date_details') }}
    WHERE date_actual BETWEEN DATEADD(month,-26, DATE_TRUNC(month, CURRENT_DATE())) AND DATEADD(month,-1, DATE_TRUNC(month, CURRENT_DATE()))
      AND day_of_month = 1
  
), bamboohr_promotions_xf AS (

    SELECT * 
    FROM "ANALYTICS"."PLUTHRA_SCRATCH_SENSITIVE"."BAMBOOHR_PROMOTIONS_XF"

), unioned AS (

    SELECT
       promotion_month,
      'company_overall' AS breakout_type,
       NULL AS division,
       NULL AS department,
       employee_id,
       percent_change
    FROM bamboohr_promotions_xf

    UNION ALL
  
    SELECT 
      promotion_month,
      'company_overall' AS breakout_type,
       division,
       NULL AS department,
       employee_id,
       percent_change
    FROM bamboohr_promotions_xf
   
    UNION ALL 
    
    SELECT 
      promotion_month,
      'company_overall' AS breakout_type,
      division,
      department,
      employee_id,
      percent_change    
    FROM bamboohr_promotions_xf

), promotions_intermediate AS (

    SELECT
      month_date,
      breakout_type,
      division,
      department,
      COUNT(employee_id) AS total_promotions,
      SUM(percent_change) AS total_percent_change
    FROM dates
    LEFT JOIN unioned
      ON dates.month_date = unioned.promotion_month
    GROUP BY 1,2,3,4

), final AS (

    SELECT
        month_date,  
        breakout_type,
        division,
        department,
        total_promotions,
        total_percent_change,
        SUM(total_promotions)  OVER (PARTITION BY breakout_type, division, department ORDER BY month_date ROWS BETWEEN 12 preceding AND CURRENT ROW)  AS total_promotions_month_rolling_12_month,
        SUM(total_percent_change)  OVER (PARTITION BY breakout_type, division, department  ORDER BY month_date ROWS BETWEEN 12 preceding AND CURRENT ROW)  AS total_percent_change_month_rolling_12_month
    FROM promotions_intermediate
    WHERE month_date >= DATEADD(month,-12, DATE_TRUNC(month, CURRENT_DATE()))

  
)

SELECT
  month_date,
  breakout_type,
  division,
  department,
  total_percent_change_month_rolling_12_month/total_promotions_month_rolling_12_month AS rolling_12_month_percent_increase_average
FROM final
