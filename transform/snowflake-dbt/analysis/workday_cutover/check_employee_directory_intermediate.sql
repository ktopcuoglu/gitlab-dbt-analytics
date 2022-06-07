WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} --pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('employee_directory_intermediate') }} --pempey_prep.sensitive.employee_directory_intermediate



),

new AS (
  SELECT *
  FROM {{ ref('workday_employee_directory_intermediate') }} --pempey_prep.sensitive.workday_employee_directory_intermediate


),

  old_prep AS (
    SELECT
      map.bhr_employee_id,
      map.wk_employee_id,
      first_name,
      last_name,
      hire_date,
      rehire_date,
      termination_date,
      hire_location_factor,
      last_work_email,
      full_name,
      date_actual,
      job_title,
      department,
      department_modified,
      department_grouping,
      division,
      division_mapped_current,
      division_grouping,
      reports_to,
      location_factor,
      locality,
      gitlab_username,
      region,
      sales_geo_differential,
      total_direct_reports,
      discretionary_bonus,
      work_email,
      cost_center,
      job_role,
      job_grade,
      jobtitle_speciality,
      is_hire_date,
      is_termination_date,
      is_rehire_date,
      employment_status,
      job_role_modified,
      is_promotion,
      tenure_days,
      layers
    FROM old
    LEFT JOIN map
      ON old.employee_id = map.bhr_employee_id
    WHERE date_actual = '2022-05-01'
    --AND employee_id NOT IN (42785, 42803)
  ),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    first_name,
    last_name,
    hire_date,
    rehire_date,
    termination_date,
    hire_location_factor,
    last_work_email,
    full_name,
    date_actual,
    job_title,
    department,
    department_modified,
    department_grouping,
    division,
    division_mapped_current,
    division_grouping,
    reports_to,
    location_factor,
    locality,
    gitlab_username,
    region,
    sales_geo_differential,
    total_direct_reports,
    discretionary_bonus,
    work_email,
    cost_center,
    job_role,
    job_grade,
    jobtitle_speciality,
    is_hire_date,
    is_termination_date,
    is_rehire_date,
    employment_status,
    job_role_modified,
    is_promotion,
    tenure_days,
    layers
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

  minused.first_name,
  new_prep.first_name,
  minused.first_name = new_prep.first_name AS matched_first_name,
  minused.last_name,
  new_prep.last_name,
  minused.last_name = new_prep.last_name AS matched_last_name,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.rehire_date,
  new_prep.rehire_date,
  minused.rehire_date = new_prep.rehire_date AS matched_rehire_date,
  minused.termination_date,
  new_prep.termination_date,
  minused.termination_date = new_prep.termination_date AS matched_termination_date,
  minused.hire_location_factor,
  new_prep.hire_location_factor,
  minused.hire_location_factor = new_prep.hire_location_factor AS matched_hire_location_factor,
  minused.last_work_email,
  new_prep.last_work_email,
  minused.last_work_email = new_prep.last_work_email AS matched_last_work_email,
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.job_title,
  new_prep.job_title,
  minused.job_title = new_prep.job_title AS matched_job_title,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.department_modified,
  new_prep.department_modified,
  minused.department_modified = new_prep.department_modified AS matched_department_modified,
  minused.department_grouping,
  new_prep.department_grouping,
  minused.department_grouping = new_prep.department_grouping AS matched_department_grouping,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division,
  minused.division_mapped_current,
  new_prep.division_mapped_current,
  minused.division_mapped_current = new_prep.division_mapped_current AS matched_division_mapped_current,
  minused.division_grouping,
  new_prep.division_grouping,
  minused.division_grouping = new_prep.division_grouping AS matched_division_grouping,
  minused.reports_to,
  new_prep.reports_to,
  minused.reports_to = new_prep.reports_to AS matched_reports_to,
  minused.location_factor,
  new_prep.location_factor,
  minused.location_factor = new_prep.location_factor AS matched_location_factor,
  minused.locality,
  new_prep.locality,
  minused.locality = new_prep.locality AS matched_locality,
  minused.gitlab_username,
  new_prep.gitlab_username,
  minused.gitlab_username = new_prep.gitlab_username AS matched_gitlab_username,
  minused.region,
  new_prep.region,
  minused.region = new_prep.region AS matched_region,
  minused.sales_geo_differential,
  new_prep.sales_geo_differential,
  minused.sales_geo_differential = new_prep.sales_geo_differential AS matched_sales_geo_differential,
  minused.total_direct_reports,
  new_prep.total_direct_reports,
  minused.total_direct_reports = new_prep.total_direct_reports AS matched_total_direct_reports,
  minused.discretionary_bonus,
  new_prep.discretionary_bonus,
  minused.discretionary_bonus = new_prep.discretionary_bonus AS matched_discretionary_bonus,
  minused.work_email,
  new_prep.work_email,
  minused.work_email = new_prep.work_email AS matched_work_email,
  minused.cost_center,
  new_prep.cost_center,
  minused.cost_center = new_prep.cost_center AS matched_cost_center,
  minused.job_role,
  new_prep.job_role,
  minused.job_role = new_prep.job_role AS matched_job_role,
  minused.job_grade,
  new_prep.job_grade,
  minused.job_grade = new_prep.job_grade AS matched_job_grade,
  minused.jobtitle_speciality,
  new_prep.jobtitle_speciality,
  minused.jobtitle_speciality = new_prep.jobtitle_speciality AS matched_jobtitle_speciality,
  minused.is_hire_date,
  new_prep.is_hire_date,
  minused.is_hire_date = new_prep.is_hire_date AS matched_is_hire_date,
  minused.is_termination_date,
  new_prep.is_termination_date,
  minused.is_termination_date = new_prep.is_termination_date AS matched_is_termination_date,
  minused.is_rehire_date,
  new_prep.is_rehire_date,
  minused.is_rehire_date = new_prep.is_rehire_date AS matched_is_rehire_date,
  minused.employment_status,
  new_prep.employment_status,
  minused.employment_status = new_prep.employment_status AS matched_employment_status,
  minused.job_role_modified,
  new_prep.job_role_modified,
  minused.job_role_modified = new_prep.job_role_modified AS matched_job_role_modified,
  minused.is_promotion,
  new_prep.is_promotion,
  minused.is_promotion = new_prep.is_promotion AS matched_is_promotion,
  minused.tenure_days,
  new_prep.tenure_days,
  minused.tenure_days = new_prep.tenure_days AS matched_tenure_days,
  minused.layers,
  new_prep.layers,
  minused.layers = new_prep.layers AS matched_layers
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.date_actual = new_prep.date_actual