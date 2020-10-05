{{ config({
    "schema": "temporary"
    })
}}

WITH job_info AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_info') }}

), bamboo_mapping AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}

), job_role AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_role') }}

), current_division_department_mapping AS (

    SELECT DISTINCT 
      division, 
      department,
      COUNT(bamboo_mapping.employee_id) AS total_employees
    FROM bamboo_mapping
    LEFT JOIN job_info
      ON job_info.employee_id = bamboo_mapping.employee_id
    WHERE CURRENT_DATE() BETWEEN effective_date AND COALESCE(effective_end_date, CURRENT_DATE())
      AND bamboo_mapping.termination_date IS NULL
    GROUP BY 1,2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY total_employees DESC) =1 
    --to account for individuals that have not been transistioned to new division
  
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
