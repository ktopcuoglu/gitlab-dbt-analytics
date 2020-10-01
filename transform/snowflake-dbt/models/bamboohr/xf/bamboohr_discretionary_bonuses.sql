{{ config({
    "schema": "analytics"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('bamboohr_custom_bonus') }}

), division_department_mapping AS (

    SELECT
  FROM {{ ref('bamboohr_job_info_current_division_base') }}

), filtered AS (

    SELECT
      employee_id,
      bonus_id,
      bonus_date,
      bonus_nominator_type,
      department,
      division_mapped_current AS division
    FROM source
    LEFT JOIN department_info
      ON employee_directory.employee_id = department_info.employee_id
      AND date_actual BETWEEN bonus_date AND COALESCE(effective_end_date::DATE, {{max_date_in_bamboo_analyses()}})
    WHERE bonus_type = 'Discretionary Bonus'

)

SELECT *
FROM filtered
