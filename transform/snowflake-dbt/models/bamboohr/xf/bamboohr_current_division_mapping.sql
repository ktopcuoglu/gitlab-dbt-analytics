WITH current_division_department_mapping AS (

    SELECT *
    FROM {{ ref ('cost_center_division_department_mapping_current') }}

), bamboo_mapping AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}

), job_info AS (

    SELECT *
    FROM {{ ref ('bamboohr_job_info') }}

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
  IFF(termination_date IS NOT NULL AND effective_end_date IS NULL, termination_date, effective_end_date) AS effective_end_date,
  division_mapped_current,
  termination_Date
FROM mapped_division