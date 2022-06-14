WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('employee_locality') }} -- pempey_prep.sensitive.employee_locality

),

new AS (
  SELECT *
  FROM {{ ref('workday_employee_locality') }} -- pempey_prep.sensitive.workday_employee_locality

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    old.updated_at,
    LOWER(old.bamboo_locality) AS bamboo_locality,
    old.location_factor
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    new.updated_at,
    LOWER(new.bamboo_locality) AS bamboo_locality,
    new.location_factor
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
  minused.updated_at,
  new_prep.updated_at,
  minused.updated_at = new_prep.updated_at AS matched_updated_at,
  minused.bamboo_locality,
  new_prep.bamboo_locality,
  minused.bamboo_locality = new_prep.bamboo_locality AS matched_bamboo_locality,
  minused.location_factor,
  new_prep.location_factor,
  minused.location_factor = new_prep.location_factor AS matched_location_factor
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.updated_at = new_prep.updated_at
ORDER BY bhr_employee_id
