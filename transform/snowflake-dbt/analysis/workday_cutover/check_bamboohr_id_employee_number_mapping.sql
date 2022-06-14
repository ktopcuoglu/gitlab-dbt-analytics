WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping') }} -- pempey_prep.sensitive.bamboohr_id_employee_number_mapping
  --where employee_id = 42663


),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_id_employee_number_mapping') }} -- pempey_prep.sensitive.workday_bamboohr_id_employee_number_mapping
  --where employee_id = 12189

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,

    first_name,
    last_name,
    hire_date,
    termination_date,
    age_cohort,
    gender,
    ethnicity,
    country,
    nationality,
    region,
    region_modified,
    gender_region,
    greenhouse_candidate_id,
    last_updated_date,
    urg_group

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
    first_name,
    last_name,
    hire_date,
    termination_date,
    age_cohort,
    gender,
    ethnicity,
    country,
    nationality,
    region,
    region_modified,
    gender_region,
    greenhouse_candidate_id,
    last_updated_date,
    urg_group

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

  minused.first_name,
  new_prep.first_name,
  minused.first_name = new_prep.first_name AS matched_first_name,
  minused.last_name,
  new_prep.last_name,
  minused.last_name = new_prep.last_name AS matched_last_name,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.termination_date,
  new_prep.termination_date,
  minused.termination_date = new_prep.termination_date AS matched_termination_date,
  minused.age_cohort,
  new_prep.age_cohort,
  minused.age_cohort = new_prep.age_cohort AS matched_age_cohort,
  minused.gender,
  new_prep.gender,
  minused.gender = new_prep.gender AS matched_gender,
  minused.ethnicity,
  new_prep.ethnicity,
  minused.ethnicity = new_prep.ethnicity AS matched_ethnicity,
  minused.country,
  new_prep.country,
  minused.country = new_prep.country AS matched_country,
  minused.nationality,
  new_prep.nationality,
  minused.nationality = new_prep.nationality AS matched_nationality,
  minused.region,
  new_prep.region,
  minused.region = new_prep.region AS matched_region,
  minused.region_modified,
  new_prep.region_modified,
  minused.region_modified = new_prep.region_modified AS matched_region_modified,
  minused.gender_region,
  new_prep.gender_region,
  minused.gender_region = new_prep.gender_region AS matched_gender_region,
  minused.greenhouse_candidate_id,
  new_prep.greenhouse_candidate_id,
  minused.greenhouse_candidate_id = new_prep.greenhouse_candidate_id AS matched_greenhouse_candidate_id,
  minused.last_updated_date,
  new_prep.last_updated_date,
  minused.last_updated_date = new_prep.last_updated_date AS matched_last_updated_date,
  minused.urg_group,
  new_prep.urg_group,
  minused.urg_group = new_prep.urg_group AS matched_urg_group
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id