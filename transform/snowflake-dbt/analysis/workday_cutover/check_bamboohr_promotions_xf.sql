WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_promotions_xf') }} -- pempey_prep.sensitive.bamboohr_promotions_xf



),

new AS (
  SELECT *
  FROM {{ ref('map_emploworkday_bamboohr_promotions_xfyee_id') }} -- pempey_prep.sensitive.workday_bamboohr_promotions_xf


),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    promotion_date,
    promotion_month,
    full_name,
    division,
    division_grouping,
    department,
    department_grouping,
    job_title,
    variable_pay,
    CEIL(new_compensation_value_usd) AS new_compensation_value_usd,
    CEIL(prior_compensation_value_usd) AS prior_compensation_value_usd,
    CEIL(change_in_comp_usd) AS change_in_comp_usd,
    CEIL(ote_usd) AS ote_usd,
    CEIL(prior_ote_usd) AS prior_ote_usd,
    CEIL(ote_change) AS ote_change,
    CEIL(total_change_in_comp) AS total_change_in_comp,
    percent_change_in_comp

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
    promotion_date,
    promotion_month,
    full_name,
    division,
    division_grouping,
    department,
    department_grouping,
    job_title,
    variable_pay,
    CEIL(new_compensation_value_usd) AS new_compensation_value_usd,
    CEIL(prior_compensation_value_usd) AS prior_compensation_value_usd,
    CEIL(change_in_comp_usd) AS change_in_comp_usd,
    CEIL(ote_usd) AS ote_usd,
    CEIL(prior_ote_usd) AS prior_ote_usd,
    CEIL(ote_change) AS ote_change,
    CEIL(total_change_in_comp) AS total_change_in_comp,
    percent_change_in_comp
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

  minused.promotion_date,
  new_prep.promotion_date,
  minused.promotion_date = new_prep.promotion_date AS matched_promotion_date,
  minused.promotion_month,
  new_prep.promotion_month,
  minused.promotion_month = new_prep.promotion_month AS matched_promotion_month,
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division,
  minused.division_grouping,
  new_prep.division_grouping,
  minused.division_grouping = new_prep.division_grouping AS matched_division_grouping,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.department_grouping,
  new_prep.department_grouping,
  minused.department_grouping = new_prep.department_grouping AS matched_department_grouping,
  minused.job_title,
  new_prep.job_title,
  minused.job_title = new_prep.job_title AS matched_job_title,
  minused.variable_pay,
  new_prep.variable_pay,
  minused.variable_pay = new_prep.variable_pay AS matched_variable_pay,
  minused.new_compensation_value_usd,
  new_prep.new_compensation_value_usd,
  minused.new_compensation_value_usd = new_prep.new_compensation_value_usd AS matched_new_compensation_value_usd,
  minused.prior_compensation_value_usd,
  new_prep.prior_compensation_value_usd,
  minused.prior_compensation_value_usd = new_prep.prior_compensation_value_usd AS matched_prior_compensation_value_usd,
  minused.change_in_comp_usd,
  new_prep.change_in_comp_usd,
  minused.change_in_comp_usd = new_prep.change_in_comp_usd AS matched_change_in_comp_usd,
  minused.ote_usd,
  new_prep.ote_usd,
  minused.ote_usd = new_prep.ote_usd AS matched_ote_usd,
  minused.prior_ote_usd,
  new_prep.prior_ote_usd,
  minused.prior_ote_usd = new_prep.prior_ote_usd AS matched_prior_ote_usd,
  minused.ote_change,
  new_prep.ote_change,
  minused.ote_change = new_prep.ote_change AS matched_ote_change,
  minused.total_change_in_comp,
  new_prep.total_change_in_comp,
  minused.total_change_in_comp = new_prep.total_change_in_comp AS matched_total_change_in_comp,
  minused.percent_change_in_comp,
  new_prep.percent_change_in_comp,
  minused.percent_change_in_comp = new_prep.percent_change_in_comp AS matched_percent_change_in_comp

FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  and minused.promotion_date = new_prep.promotion_date