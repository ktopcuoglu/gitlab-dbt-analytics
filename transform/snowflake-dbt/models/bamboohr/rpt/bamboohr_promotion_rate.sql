{{ config({
    "schema": "analytics"
    })
}}

WITH promotions AS (

    SELECT *
    FROM {{ ref ('bamboohr_promotions_xf') }}
    WHERE promotion_month BETWEEN DATEADD(month,-12, DATE_TRUNC(month, CURRENT_DATE())) AND
                                  DATEADD(month,-1, DATE_TRUNC(month, CURRENT_DATE()))

), bamboohr_headcount_aggregated AS (

    SELECT 
      month_date, 
      IFF(breakout_type = 'kpi_breakout','company_breakout',breakout_type) AS breakout_type, 
      CASE WHEN breakout_type = 'kpi_breakout' 
             THEN 'Company Overall - Including Sales Development'
           WHEN breakout_type = 'division_breakout' 
             THEN division
           ELSE department END                                             AS division_department, 
      division,
      department,
      headcount_end, 
      rolling_12_month_promotions
    FROM {{ ref ('bamboohr_rpt_headcount_aggregation') }}
    WHERE breakout_type in ('department_breakout','kpi_breakout','division_breakout')
      AND eeoc_field_name = 'no_eeoc'

), current_division_department AS (

    SELECT 
      DISTINCT division_mapped_current, 
      department_modified
    FROM {{ ref ('bamboohr_job_info_current_division_base') }}

), marketing_headcount_excluding_sales_development AS (

    SELECT 
      month_date, 
      'Marketing - Excluding Sales Development'          AS division_department,
      SUM(headcount_end)                                 AS headcount_end, 
      SUM(rolling_12_month_promotions)                   AS rolling_12_month_promotions
    FROM bamboohr_headcount_aggregated
    WHERE breakout_type in ('department_breakout')
      AND department != 'Sales Development'
      AND division = 'Marketing'
      GROUP BY 1,2

), company_headcount_excluding_sales_development AS (

    SELECT 
      month_date, 
      'Company - Excluding Sales Development'           AS division_department,
      SUM(headcount_end)                                AS headcount_end, 
      SUM(rolling_12_month_promotions)                  AS rolling_12_month_promotions
    FROM bamboohr_headcount_aggregated
    WHERE breakout_type in ('department_breakout')
      AND department != 'Sales Development'
    GROUP BY 1

), division AS (

    SELECT
      promotion_month,
      'division_breakout'                                AS breakout_type,
      division                                           AS division_department,
      AVG(promotions.percent_change_in_comp)             AS average_increase,
      MEDIAN(promotions.percent_change_in_comp)          AS median_increase
    FROM promotions
    GROUP BY 1,2,3

    UNION ALL

    SELECT
      promotion_month,
      'division_breakout'                                AS breakout_type,
      'Marketing - Excluding Sales Development'          AS division_department,
      AVG(percent_change_in_comp)                        AS average_increase,
      MEDIAN(percent_change_in_comp)                     AS median_increase
    FROM promotions
    WHERE department != 'Sales Development'
      AND division = 'Marketing'
    GROUP BY 1,2,3

), company AS (

    SELECT
      promotion_month,
      'company_breakout'                                  AS breakout_type,
      'Company Overall - Including Sales Development'     AS division_department,
      AVG(percent_change_in_comp)                         AS average_increase,
      MEDIAN(percent_change_in_comp)                      AS median_increase
    FROM promotions
    GROUP BY 1,2,3
    
    UNION ALL 

    SELECT
      promotion_month,
      'company_breakout'                                  AS breakout_type,
      'Company - Excluding Sales Development'             AS division_department,
      AVG(percent_change_in_comp)                         AS average_increase,
      MEDIAN(percent_change_in_comp)                      AS median_increase
    FROM promotions
    WHERE department != 'Sales Development'
    GROUP BY 1,2,3
    
), department AS (

    SELECT
      promotion_month,
      'department_breakout'                               AS breakout_type,
      department                                          AS division_department,
      AVG(percent_change_in_comp)                         AS average_increase,
      MEDIAN(percent_change_in_comp)                      AS median_incrase
    FROM promotions
    WHERE department != 'Sales Development'
    GROUP BY 1,2,3

), unioned AS (

    SELECT *
    FROM division

    UNION ALL

    SELECT *
    FROM company

    UNION ALL 

    SELECT *
    FROM department

), final AS (

    SELECT
      unioned.*,
      COALESCE(bamboohr_headcount_aggregated.headcount_end,
               marketing_headcount_excluding_sales_development.headcount_end,
               company_headcount_excluding_sales_development.headcount_end)                 AS headcount_end,
      COALESCE(bamboohr_headcount_aggregated.rolling_12_month_promotions,
               marketing_headcount_excluding_sales_development.rolling_12_month_promotions,
               company_headcount_excluding_sales_development.rolling_12_month_promotions)   AS rolling_12_month_promotions 
    FROM unioned
    LEFT JOIN bamboohr_headcount_aggregated
      ON bamboohr_headcount_aggregated.breakout_type = unioned.breakout_type
      AND bamboohr_headcount_aggregated.division_department = unioned.division_department
      AND bamboohr_headcount_aggregated.month_date = unioned.promotion_month
    LEFT JOIN marketing_headcount_excluding_sales_development
      ON marketing_headcount_excluding_sales_development.division_department = unioned.division_department
      AND bamboohr_headcount_aggregated.month_date = unioned.promotion_month
    LEFT JOIN company_headcount_excluding_sales_development
      ON company_headcount_excluding_sales_development.division_department = unioned.division_department      
      AND bamboohr_headcount_aggregated.month_date = unioned.promotion_month


)

SELECT * 
FROM final
