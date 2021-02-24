{% set lines_to_repeat =
          "DATE_TRUNC(month,hire_date_mod)                                                           AS hire_month,
          SUM(IFF(job_opening_type = 'New Hire' AND hire_type != 'Transfer',1,0))                    AS new_hire,
          SUM(IFF(job_opening_type = 'New Hire' AND hire_type = 'Transfer',1,0))                     AS new_position_filled_internally,
          SUM(IFF(job_opening_type IN 
                    ('Current Team Member','Internal Transfer'),1,0))                                AS transfers,
          SUM(IFF(job_opening_type IN ('Backfill'),1,0))                                             AS backfill,
          SUM(IFF(job_opening_type IS NULL,1,0))                                                     AS unidentified_job_opening_type,
          COUNT(*)                                                                                   AS total_greenhouse_reqs_filled
        FROM greenhouse_hire_type
        WHERE hired_in_bamboohr= TRUE
        GROUP BY 1,2,3,4" %}

WITH dim_date AS (

    SELECT DISTINCT 
      fiscal_year, 
      last_day_of_month AS month_date
    FROM {{ ref ('dim_date') }}
 
), headcount AS (
  
    SELECT 
      month_date, 
      CASE WHEN breakout_type = 'kpi_breakout' 
            THEN 'all_company_breakout'
           WHEN breakout_type = 'department_breakout' 
            THEN 'department_division_breakout'
           ELSE breakout_type END                                            AS breakout_type,
      IFF(breakout_type = 'kpi_breakout','all_company_breakout', department) AS department,
      IFF(breakout_type = 'kpi_breakout','all_company_breakout', division)   AS division,
      COALESCE(headcount_end,0)                                              AS headcount_actual,
      COALESCE(hire_count,0)                                                 AS hires_actual
    FROM {{ ref ('bamboohr_rpt_headcount_aggregation') }}
    WHERE breakout_type IN ('kpi_breakout','department_breakout','division_breakout')
      AND eeoc_field_name = 'no_eeoc'
  
), hire_plan AS (

    SELECT *,
      IFF(DATE_TRUNC(month, month_date) = DATE_TRUNC(month, DATEADD(month, -1, CURRENT_DATE())),1,0) AS last_month
    FROM {{ ref ('hire_replan_xf') }}

), department_name_changes AS (

    SELECT
      TRIM(old_department_name) AS old_department_name,	
      TRIM(new_department_name) AS new_department_name,	
      change_effective_date
    FROM {{ ref ('department_name_changes') }}

), greenhouse_hire_type AS (

    SELECT *
    FROM {{ ref ('greenhouse_hires') }} 

), hire_type_aggregated AS (

    SELECT
      'department_division_breakout'                                AS breakout_type,
      division,
      department,
      {{lines_to_repeat}} 

    UNION ALL  
    
    SELECT
      'division_breakout'                                           AS breakout_type,
      division,
      'division_breakout'                                           AS department,
      {{lines_to_repeat}} 

    UNION ALL
    
    SELECT
      'all_company_breakout'                                        AS breakout_type,
      'all_company_breakout'                                        AS division,
      'all_company_breakout'                                        AS department,
      {{lines_to_repeat}} 

), joined AS (

    SELECT 
      dim_date.fiscal_year,
      hire_plan.month_date,
      hire_plan.breakout_type,
      COALESCE(TRIM(department_name_changes.new_department_name), hire_plan.department) AS department,
      hire_plan.division,
      hire_plan.planned_headcount,
      hire_plan.planned_hires,
      COALESCE(headcount.headcount_actual,0)                                       AS headcount_actual,
      COALESCE(headcount.hires_actual,0)                                           AS hires_actual,
      IFF(hire_plan.planned_headcount = 0, NULL, 
        ROUND((headcount.headcount_actual/hire_plan.planned_headcount),4))         AS actual_headcount_vs_planned_headcount,   

      new_hire,
      transfers,
      backfill,
      unidentified_job_opening_type,
      total_greenhouse_reqs_filled,
      new_hire + backfill                                                         AS total_hires_greenhouse
    FROM dim_date
    LEFT JOIN hire_plan
      ON dim_date.month_date = hire_plan.month_date
    LEFT JOIN department_name_changes
      ON department_name_changes.old_department_name = hire_plan.department
    LEFT JOIN headcount
      ON headcount.breakout_type = hire_plan.breakout_type
      AND headcount.department = COALESCE(department_name_changes.new_department_name, hire_plan.department)
      AND headcount.division = hire_plan.division
      AND headcount.month_date = DATE_TRUNC(month, hire_plan.month_date)
    LEFT JOIN hire_type_aggregated
      ON hire_type_aggregated.breakout_type = hire_plan.breakout_type
      AND hire_type_aggregated.department = COALESCE(department_name_changes.new_department_name, hire_plan.department)
      AND hire_type_aggregated.division = hire_plan.division
      AND hire_type_aggregated.hire_month = DATE_TRUNC(month, hire_plan.month_date)

), final AS (

    SELECT *,
      SUM(planned_hires) OVER 
            (PARTITION BY fiscal_year, breakout_type, division, department
            ORDER BY month_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   AS cumulative_planned_hires,
      SUM(hires_actual) OVER
            (PARTITION BY fiscal_year, breakout_type, division, department
            ORDER BY month_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   AS cumulative_hires_actual,
        IFF(cumulative_planned_hires = 0, NULL,
             ROUND((cumulative_hires_actual/cumulative_planned_hires),2))           AS cumulative_hires_vs_plan
    FROM joined
    WHERE month_date BETWEEN DATEADD(month, -24, CURRENT_DATE()) AND DATEADD(month, 12, CURRENT_DATE())
)

SELECT *
FROM final
