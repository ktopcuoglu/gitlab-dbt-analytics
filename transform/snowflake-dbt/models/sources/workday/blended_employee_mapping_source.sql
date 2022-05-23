WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_employee_mapping_source') }} -- need a daily snapshot
)

SELECT
  employee_number,
  employee_number AS employee_id,
  first_name,
  last_name,
  hire_date,
  termination_date,
  greenhouse_candidate_id,
  cost_center,
  gitlab_username,
  jobtitle_speciality_single_select,
  jobtitle_speciality_multi_select,
  locality,
  nationality,
  gender_dropdown,
  region,
  job_role,
  sales_geo_differential,
  date_of_birth,
  employee_status_date,
  employment_history_status,
  ethnicity,
  gender,
  country,
  age,
  TRY_TO_NUMBER(job_grade) AS job_grade,
  pay_frequency,
  uploaded_at,
  uploaded_row_number_desc,
  'bamboohr' AS source_system
FROM bamboohr

UNION 

SELECT
  employee_id AS employee_number,
  employee_id,
  first_name,
  last_name,
  hire_date,
  termination_date,
  TRY_TO_NUMBER(greenhouse_candidate_id) AS greenhouse_candidate_id, -- may only need the conversion for testing
  cost_center,
  gitlab_username,
  jobtitle_speciality_single_select,
  jobtitle_speciality_multi_select,
  locality,
  nationality,
  gender_dropdown,
  region,
  job_role,
  sales_geo_differential,
  date_of_birth,
  employee_status_date,
  employment_history_status,
  ethnicity,
  gender,
  country,
  age,
  job_grade,
  pay_frequency,
  uploaded_at,
  DENSE_RANK() OVER (ORDER BY uploaded_at DESC) AS uploaded_row_number_desc,
  'workday' AS source_system
FROM workday