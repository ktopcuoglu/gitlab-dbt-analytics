WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_employment_status_xf') }} -- pempey_prep.sensitive.bamboohr_employment_status_xf
  --where employee_id = 42663


),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_employment_status_xf') }} -- pempey_prep.sensitive.workday_bamboohr_employment_status_xf
  --where employee_id = 12189

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    employment_status,
    termination_type,
    is_rehire,
    next_employment_status,
    valid_from_date,
    valid_to_date
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
    employment_status,
    termination_type,
    is_rehire,
    next_employment_status,
    valid_from_date,
    valid_to_date

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

  minused.employment_status,
  new_prep.employment_status,
  minused.employment_status = new_prep.employment_status AS matched_employment_status,
  minused.termination_type,
  new_prep.termination_type,
  minused.termination_type = new_prep.termination_type AS matched_termination_type,
  minused.is_rehire,
  new_prep.is_rehire,
  minused.is_rehire = new_prep.is_rehire AS matched_is_rehire,
  minused.next_employment_status,
  new_prep.next_employment_status,
  minused.next_employment_status = new_prep.next_employment_status AS matched_next_employment_status,
  minused.valid_from_date,
  new_prep.valid_from_date,
  minused.valid_from_date = new_prep.valid_from_date AS matched_valid_from_date,
  minused.valid_to_date,
  new_prep.valid_to_date,
  minused.valid_to_date = new_prep.valid_to_date AS matched_valid_to_date
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.valid_from_date = new_prep.valid_from_date