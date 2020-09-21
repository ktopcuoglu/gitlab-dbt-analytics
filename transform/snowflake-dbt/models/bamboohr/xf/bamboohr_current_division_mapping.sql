WITH current_division_department_mapping AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS"."COST_CENTER_DIVISION_DEPARTMENT_MAPPING_CURRENT"
  
), bamboo_mapping AS (

    SELECT *
    FROM "ANALYTICS"."PLUTHRA_SCRATCH_SENSITIVE"."BAMBOOHR_ID_EMPLOYEE_NUMBER_MAPPING"

), job_info AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_INFO"
  
), mapped_division AS (

    SELECT 
      job_info.*, 
      CASE WHEN job_info.department IN ('People','People Ops') 
            THEN 'People Group'
           ELSE COALESCE(current_division_department_mapping.division, job_info.division) END AS division_mapped_current,
      bamboo_mapping.termination_date   
    FROM job_info 
    LEFT JOIN current_division_department_mapping
        ON current_division_department_mapping.department = job_info.department    
    LEFT JOIN bamboo_mapping
      ON bamboo_mapping.employee_id = job_info.employee_id
    
)

SELECT
  job_id,
  employee_id,
  effective_date,
  IFF(termination_date IS NOT NULL AND effective_end_date IS NULL, termination_date, effective_end_date) AS effective_end_Date,
  division_mapped_current,
  termination_Date
FROM mapped_division