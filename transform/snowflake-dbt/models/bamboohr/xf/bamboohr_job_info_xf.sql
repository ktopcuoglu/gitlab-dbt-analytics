{# {{ config({
    "schema": "temporary"
    })
}} #}

WITH job_info AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_INFO"

), bamboo_mapping AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_ID_EMPLOYEE_NUMBER_MAPPING"

), job_role AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_ROLE"


), current_division_department_mapping AS (

    SELECT DISTINCT 
      division, 
      department
    FROM bamboo_mapping
    LEFT JOIN job_info
      ON job_info.employee_id = bamboo_mapping.employee_id
    WHERE CURRENT_DATE() BETWEEN effective_date AND COALESCE(effective_end_date, CURRENT_DATE())
      AND bamboo_mapping.termination_date IS NULL
  
)

SELECT 
  job_info.*, 
  CASE WHEN job_info.department IN ('People','People Ops') 
       THEN 'People Group'
       ELSE COALESCE(current_division_department_mapping.division, job_info.division) END AS division_mapped_current,
  bamboo_mapping.termination_date   
FROM bamboo_mapping
LEFT JOIN job_info 
  ON job_info.employee_id = bamboo_mapping.employee_id
LEFT JOIN current_division_department_mapping
  ON current_division_department_mapping.department = job_info.department
