WITH dates AS (

    SELECT 
      date_actual AS month_date,
      1 AS join
    FROM {{ ref ('date_details') }}
    WHERE date_actual BETWEEN DATEADD(MONTH,-26, DATE_TRUNC(MONTH, CURRENT_DATE())) AND DATEADD(MONTH,-1, DATE_TRUNC(MONTH, CURRENT_DATE()))
      AND day_of_month = 1
  
), bamboohr_promotions_xf AS (

    SELECT * 
    FROM {{ ref ('bamboohr_promotions_xf') }}

), current_division_department AS (
  
    SELECT 
      DISTINCT division_mapped_current AS division, 
      department, 
      1 as join
    FROM {{ ref ('bamboohr_job_info_current_division_base') }}  
    WHERE DATE_TRUNC(MONTH, effective_date) = DATEADD(MONTH,-1, DATE_TRUNC(MONTH, CURRENT_DATE()))
  
), base as (

    SELECT 
      dates.month_date, 
      division, 
      department
    FROM dates
    LEFT JOIN current_division_department
      ON dates.join = current_division_department.join

), joined AS (

    SELECT 
      base.*, 
      promotion_date, 
      employee_id,
      total_change,
      percent_change
    FROM base
    LEFT JOIN bamboohr_promotions_xf
      ON base.month_date = bamboohr_promotions_xf.promotion_month
      AND base.division = bamboohr_promotions_xf.division
      AND base.department = bamboohr_promotions_xf.department

), unioned AS (

    SELECT DISTINCT
       month_date,
      'company_breakout'    AS breakout_type,
       NULL                 AS division,
       NULL                 AS department,
       COUNT(employee_id)   AS total_promotions,
       SUM(percent_change)  AS total_percent_change
    FROM joined 
    GROUP BY 1,2,3
    
    UNION ALL
  
    SELECT DISTINCT
       month_date,
      'division_breakout'           AS breakout_type,
       division,
       NULL                         AS department,
       COUNT(employee_id)           AS total_promotions,
       SUM(percent_change)          AS total_percent_change
    FROM joined 
    GROUP BY 1,2,3
 
    UNION ALL 
    
    SELECT DISTINCT
       month_date,
      'department_breakout'         AS breakout_type,
       division,
       department,
       COUNT(employee_id)           AS total_promotions,
       SUM(percent_change)          AS total_percent_change
    FROM joined 
    GROUP BY 1,2,3,4
  
), final AS (

    SELECT
      month_date,  
      breakout_type,
      division,
      department,
      total_promotions,
      total_percent_change,
      SUM(total_promotions) OVER 
          (PARTITION BY breakout_type, division, department ORDER BY month_date ROWS BETWEEN 12 preceding AND CURRENT ROW) AS total_promotions_month_rolling_12_month,
      SUM(total_percent_change) OVER 
          (PARTITION BY breakout_type, division, department  ORDER BY month_date ROWS BETWEEN 12 preceding AND CURRENT ROW) AS total_percent_change_month_rolling_12_month
    FROM unioned
    WHERE month_date >= DATEADD(MONTH,-12, DATE_TRUNC(MONTH, CURRENT_DATE()))
  
)

SELECT 
  month_date,
  breakout_type,
  division,
  department,
  total_promotions_month_rolling_12_month,
  total_percent_change_month_rolling_12_month/total_promotions_month_rolling_12_month AS rolling_12_month_percent_increase_average
FROM final

