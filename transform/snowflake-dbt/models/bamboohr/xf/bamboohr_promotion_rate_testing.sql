WITH promotions AS (

    SELECT *
    FROM {{ ref('bamboohr_promotions_xf') }}

), bamboohr_base AS (

    SELECT
      DATEADD(month,-11, date_actual)                           AS rolling_start_month,
      date_actual                                               AS rolling_end_month,
      field_name,
      field_value
    FROM {{ ref('bamboohr_base_mapping') }}
  
), headcount_end AS (

    SELECT 
      month_date, 
      CASE WHEN breakout_type = 'kpi_breakout'
            THEN 'company_breakout'
           WHEN breakout_type = 'department_breakout'
            THEN 'department_grouping_breakout'
           WHEN breakout_type ='division_breakout'
            THEN 'division_grouping_breakout' 
           ELSE NULL END                                                        AS breakout_type, 
      CASE WHEN breakout_type = 'kpi_breakout' 
             THEN 'company_breakout'
           WHEN breakout_type = 'division_breakout' 
             THEN {{bamboohr_division_grouping(division='division')}}
           ELSE {{bamboohr_department_grouping(department='department')}} END   AS division_department, 
      SUM(headcount_end)                                                        AS headcount_end
    FROM {{ ref('bamboohr_rpt_headcount_aggregation') }}  
    WHERE breakout_type IN ('department_breakout','kpi_breakout','division_breakout')
        AND eeoc_field_name = 'no_eeoc'
    GROUP BY 1,2,3    
  

), joined AS (

    SELECT 
      bamboohr_base.*, 
      promotions.*, 
      headcount_end
    FROM bamboohr_base
    LEFT JOIN promotions
      ON promotions.promotion_month BETWEEN rolling_start_month AND rolling_end_month
      AND IFF(field_name = 'division_grouping_breakout', promotions.division_grouping, promotions.department_grouping) = bamboohr_base.field_value
    LEFT JOIN headcount_end
      ON headcount_end.month_date = rolling_end_month
      AND field_name = breakout_type
      AND field_value = division_department
)

SELECT
  rolling_end_month AS month_date,
  field_name, 
  field_value,
  headcount_end,
  COUNT(employee_id)                            AS total_promotions,
  total_promotions/headcount_end                AS percent_promotions,
  AVG(percent_change_in_comp)                   AS average_change_in_comp,
  MEDIAN(percent_change_in_comp)                AS median_change_change_in_comp
FROM joined
WHERE rolling_end_month = '2020-10-01'
GROUP BY 1,2,3,4
