WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_missing_emergency_contact_alert') }} -- pempey_prod.legacy.bamboohr_missing_emergency_contact_alert
  --where employee_id = 42663


),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_missing_emergency_contact_alert') }} -- pempey_prod.legacy.workday_bamboohr_missing_emergency_contact_alert
  --where employee_id = 12189

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    full_name,
    hire_date,
    last_work_email,
    total_emergency_contacts
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
  --WHERE date_actual = '2022-05-01'
  --AND employee_id NOT IN (42785, 42803)
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    full_name,
    hire_date,
    last_work_email,
    total_emergency_contacts

  FROM new
  LEFT JOIN map
    ON new.employee_id = map.wk_employee_id
  --WHERE date_actual = '2022-05-01'

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

  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.last_work_email,
  new_prep.last_work_email,
  minused.last_work_email = new_prep.last_work_email AS matched_last_work_email,
  minused.total_emergency_contacts,
  new_prep.total_emergency_contacts,
  minused.total_emergency_contacts = new_prep.total_emergency_contacts AS matched_total_emergency_contacts
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id