WITH current_division_department_mapping AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS"."COST_CENTER_DIVISION_DEPARTMENT_MAPPING_CURRENT"
  
), job_info AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_INFO"
  
), mapped_division AS (

    SELECT 
      job_info.*, 
      CASE WHEN job_info.department IN ('People','People Ops') 
            THEN 'People Group'
           ELSE COALESCE(current_division_department_mapping.division, job_info.division) END AS division_mapped_current   
    FROM job_info 
    LEFT JOIN current_division_department_mapping
        ON current_division_department_mapping.department = job_info.department    
    
)

SELECT
  job_id,
  employee_id,
  effective_date,
  effective_end_date,
  division_mapped_current
FROM mapped_division
