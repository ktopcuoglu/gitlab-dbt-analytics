{{ simple_cte([
    ('hires','greenhouse_hires'),
    ('job_departments','greenhouse_jobs_departments_source'),
    ('departments','wk_prep_greenhouse_departments')
])}}

SELECT
  hires.unique_key,
  hires.hire_date_mod               AS hire_date,
  hires.region,
  hires.division,
  hires.department,
  hires.hired_in_bamboohr,
  hires.candidate_id,
  hires.job_opening_type,
  hires.hire_type,
  departments.department_name       AS greenhouse_department_name,
  departments.level_1               AS greenhouse_department_level_1,
  departments.level_2               AS greenhouse_department_level_2,
  departments.level_3               AS greenhouse_department_level_3
FROM hires
LEFT JOIN job_departments
  ON hires.job_id = job_departments.job_id
LEFT JOIN departments
  ON job_departments.department_id = departments.department_id

