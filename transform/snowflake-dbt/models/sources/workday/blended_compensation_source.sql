WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_compensation_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_compensation_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
)

SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.effective_date,
  bamboohr.compensation_type,
  bamboohr.compensation_change_reason,
  bamboohr.pay_rate,
  bamboohr.compensation_value,
  bamboohr.compensation_currency,
  bamboohr.uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.compensation_update_id ASC) AS compensation_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  effective_date,
  compensation_type,
  compensation_change_reason,
  pay_rate,
  compensation_value, -- convert to non-annualized?
  compensation_currency,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY initiated_at ASC) AS compensation_sequence,
  'workday' AS source_system
FROM workday

/*
LEFT JOIN pay_frequency
      ON bamboohr_compensation_changes.employee_id = pay_frequency.employee_id
      AND bamboohr_compensation_changes.effective_date BETWEEN pay_frequency.effective_date AND pay_frequency.next_effective_date
*/