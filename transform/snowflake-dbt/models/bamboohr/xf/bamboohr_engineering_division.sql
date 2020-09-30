WITH bamboohr_employee_directory AS (

    SELECT *
    FROM {{ ref ('employee_directory_analysis') }}

), bamboohr_engineering_division_mapping AS (

    SELECT *
    FROM {{ ref ('bamboohr_engineering_division_mapping') }}

), development_department_employees AS (

    SELECT
      DISTINCT date_actual                                                  AS date_employed,
      employee_id,
      full_name,
      LOWER(job_title)                                                      AS job_title,
      LOWER(TRIM(VALUE::string))                                            AS jobtitle_speciality,
      reports_to,
      layers,
      department,
      work_email
    FROM bamboohr_employee_directory,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality,'&',','),''), ','))
    WHERE division = 'Engineering'
      AND date_actual >= '2020-01-01'

), engineering_team_member_attributes AS (
    
    SELECT 
      DISTINCT development_department_employees.date_employed,
      development_department_employees.employee_id,
      development_department_employees.full_name,
      development_department_employees.job_title,
      bamboohr_engineering_division_mapping.sub_department,
      development_department_employees.jobtitle_speciality,
      CASE 
        WHEN development_department_employees.employee_id IN (41965,41996,41453,41482,41974,41487,42029,40914,41954) OR development_department_employees.job_title LIKE '%backend%' THEN 'backend'
        WHEN development_department_employees.job_title LIKE '%fullstack%' THEN 'fullstack'
        WHEN development_department_employees.job_title LIKE '%frontend%' THEN 'frontend'
        ELSE NULL END AS technology_group,
      development_department_employees.department,
      development_department_employees.work_email,
      development_department_employees.reports_to
    FROM development_department_employees
    LEFT JOIN bamboohr_engineering_division_mapping
      ON development_department_employees.jobtitle_speciality = bamboohr_engineering_division_mapping.jobtitle_speciality

), reporting_structure AS (

    SELECT engineering_team_member_attributes.*,
      bamboohr_engineering_division_mapping.team_name
    FROM engineering_team_member_attributes
    LEFT JOIN bamboohr_engineering_division_mapping
      ON engineering_team_member_attributes.jobtitle_speciality = bamboohr_engineering_division_mapping.jobtitle_speciality
      ON engineering_team_member_attributes.sub_department = bamboohr_engineering_division_mapping.sub_department
      ON engineering_team_member_attributes.technology_group = bamboohr_engineering_division_mapping.technology_group
 
)

SELECT *,
FROM reporting_structure
