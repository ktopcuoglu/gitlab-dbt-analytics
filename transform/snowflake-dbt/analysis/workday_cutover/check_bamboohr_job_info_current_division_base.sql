WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_job_info_current_division_base') }} -- pempey_prod.legacy.bamboohr_job_info_current_division_base

),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_job_info_current_division_base') }} -- pempey_prod.legacy.workday_bamboohr_job_info_current_division_base

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    old.job_title,
    old.effective_date,
    old.effective_end_date,
    old.department,
    old.division,
    old.entity,
    old.reports_to,
    old.job_role,
    old.division_mapped_current,
    old.division_grouping,
    old.department_modified,
    old.department_grouping,
    old.termination_date
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    new.job_title,
    new.effective_date,
    new.effective_end_date,
    new.department,
    new.division,
    new.entity,
    new.reports_to,
    new.job_role,
    new.division_mapped_current,
    new.division_grouping,
    new.department_modified,
    new.department_grouping,
    new.termination_date
  FROM new
  LEFT JOIN map
    ON new.employee_id = map.wk_employee_id


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
  minused.job_title,
  new_prep.job_title,
  minused.job_title = new_prep.job_title AS matched_job_title,
  minused.effective_date,
  new_prep.effective_date,
  minused.effective_date = new_prep.effective_date AS matched_effective_date,
  minused.effective_end_date,
  new_prep.effective_end_date,
  minused.effective_end_date = new_prep.effective_end_date AS matched_effective_end_date,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division,
  minused.entity,
  new_prep.entity,
  minused.entity = new_prep.entity AS matched_entity,
  minused.reports_to,
  new_prep.reports_to,
  minused.reports_to = new_prep.reports_to AS matched_reports_to,
  minused.job_role,
  new_prep.job_role,
  minused.job_role = new_prep.job_role AS matched_job_role,
  minused.division_mapped_current,
  new_prep.division_mapped_current,
  minused.division_mapped_current = new_prep.division_mapped_current AS matched_division_mapped_current,
  minused.division_grouping,
  new_prep.division_grouping,
  minused.division_grouping = new_prep.division_grouping AS matched_division_grouping,
  minused.department_modified,
  new_prep.department_modified,
  minused.department_modified = new_prep.department_modified AS matched_department_modified,
  minused.department_grouping,
  new_prep.department_grouping,
  minused.department_grouping = new_prep.department_grouping AS matched_department_grouping,
  minused.termination_date,
  new_prep.termination_date,
  minused.termination_date = new_prep.termination_date AS matched_termination_date

FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.effective_date = new_prep.effective_date
ORDER BY bhr_employee_id