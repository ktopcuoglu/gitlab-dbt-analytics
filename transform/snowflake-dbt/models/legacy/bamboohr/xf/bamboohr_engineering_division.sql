{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH employees AS (

    SELECT *
    FROM {{ ref('employee_directory_analysis') }}

), bamboohr_engineering_division_mapping AS (

    SELECT *
    FROM {{ ref('sheetload_product_group_mappings') }}

), engineering_employees AS (

    SELECT
      date_actual,
      employee_id,
      full_name,
      job_title                     AS job_title,
      SUBSTRING(LOWER(TRIM(VALUE::VARCHAR)),charindex(':',LOWER(TRIM(VALUE::VARCHAR)))+1,100)   AS job_title_speciality,
      reports_to,
      layers,
      department,
      work_email
    FROM employees,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality,'&',','),''), ','))
    WHERE division = 'Engineering'
      AND date_actual >= '2020-01-01'

), engineering_employee_attributes AS (
    
    SELECT 
      engineering_employees.date_actual,
      engineering_employees.employee_id,
      engineering_employees.full_name,
      engineering_employees.job_title,
      CASE
        WHEN bamboohr_engineering_division_mapping.section_name = 'sec'
          THEN 'secure' 
        ELSE bamboohr_engineering_division_mapping.section_name END as sub_department,
      engineering_employees.job_title_speciality,
      CASE 
        WHEN engineering_employees.employee_id IN (41965,41996,41453,41482,41974,41487,42029,40914,41954,46) 
            OR LOWER(engineering_employees.job_title) LIKE '%backend%' 
          THEN 'backend'
        WHEN LOWER(engineering_employees.job_title) LIKE '%fullstack%'
          THEN 'fullstack'
        WHEN LOWER(engineering_employees.job_title) LIKE '%frontend%'
          THEN 'frontend'
        ELSE NULL END               AS technology_group,
      engineering_employees.department,
      engineering_employees.work_email,
      engineering_employees.reports_to
    FROM engineering_employees
    LEFT JOIN bamboohr_engineering_division_mapping
      ON bamboohr_engineering_division_mapping.group_name = engineering_employees.job_title_speciality 

)

SELECT *
FROM engineering_employee_attributes
