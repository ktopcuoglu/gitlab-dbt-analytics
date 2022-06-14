WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} --pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('employee_directory_analysis') }} --pempey_prod.legacy.employee_directory_analysis

),

new AS (
  SELECT *
  FROM {{ ref('workday_employee_directory_analysis') }} --pempey_prod.legacy.workday_employee_directory_analysis

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    old.date_actual,
    old.reports_to,
    old.full_name,
    old.work_email,
    old.gitlab_username,
    old.region,
    LOWER(old.sales_geo_differential) AS sales_geo_differential,
    old.jobtitle_speciality,
    old.job_role_modified,
    old.is_hire_date,
    old.is_termination_date,
    old.hire_date,
    old.cost_center,
    old.layers,
    old.job_title,
    old.location_factor,
    old.department,
    old.division,
    old.exclude_from_location_factor
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
  WHERE date_actual = '2022-05-01'
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    new.date_actual,
    new.reports_to,
    new.full_name,
    new.work_email,
    new.gitlab_username,
    new.region,
    LOWER(new.sales_geo_differential) AS sales_geo_differential,
    new.jobtitle_speciality,
    new.job_role_modified,
    new.is_hire_date,
    new.is_termination_date,
    new.hire_date,
    new.cost_center,
    new.layers,
    new.job_title,
    new.location_factor,
    new.department,
    new.division,
    new.exclude_from_location_factor
  FROM new
  LEFT JOIN map
    ON new.employee_id = map.wk_employee_id
  WHERE date_actual = '2022-05-01'

),

minused AS (
  SELECT
    *
  FROM old_prep

  MINUS

  SELECT
    *
  FROM new_prep
)

SELECT
  minused.bhr_employee_id,
  minused.wk_employee_id,
  minused.date_actual,
  minused.reports_to,
  new_prep.reports_to,
  minused.reports_to = new_prep.reports_to AS matched_reports_to,
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.work_email,
  new_prep.work_email,
  minused.work_email = new_prep.work_email AS matched_work_email,
  minused.gitlab_username,
  new_prep.gitlab_username,
  minused.gitlab_username = new_prep.gitlab_username AS matched_gitlab_username,
  minused.region,
  new_prep.region,
  minused.region = new_prep.region AS matched_region,
  minused.sales_geo_differential,
  new_prep.sales_geo_differential,
  minused.sales_geo_differential = new_prep.sales_geo_differential AS matched_sales_geo_differential,
  minused.jobtitle_speciality,
  new_prep.jobtitle_speciality,
  minused.jobtitle_speciality = new_prep.jobtitle_speciality AS matched_jobtitle_speciality,
  minused.job_role_modified,
  new_prep.job_role_modified,
  minused.job_role_modified = new_prep.job_role_modified AS matched_job_role_modified,
  minused.is_hire_date,
  new_prep.is_hire_date,
  minused.is_hire_date = new_prep.is_hire_date AS matched_is_hire_date,
  minused.is_termination_date,
  new_prep.is_termination_date,
  minused.is_termination_date = new_prep.is_termination_date AS matched_is_termination_date,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.cost_center,
  new_prep.cost_center,
  minused.cost_center = new_prep.cost_center AS matched_cost_center,
  minused.layers,
  new_prep.layers,
  minused.layers = new_prep.layers AS matched_layers,
  minused.job_title,
  new_prep.job_title,
  minused.job_title = new_prep.job_title AS matched_job_title,
  minused.location_factor,
  new_prep.location_factor,
  minused.location_factor = new_prep.location_factor AS matched_location_factor,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division,
  minused.exclude_from_location_factor,
  new_prep.exclude_from_location_factor,
  minused.exclude_from_location_factor = new_prep.exclude_from_location_factor AS matched_exclude_from_location_factor
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.date_actual = new_prep.date_actual