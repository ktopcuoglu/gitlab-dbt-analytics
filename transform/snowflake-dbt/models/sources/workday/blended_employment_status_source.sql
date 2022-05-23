WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_employment_status_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_employment_status_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
)

SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.effective_date,
  bamboohr.employment_status,
  bamboohr.termination_type,
  bamboohr.uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY map.wk_employee_id, bamboohr.effective_date ORDER BY bamboohr.status_id ASC) AS employment_status_sequence,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  effective_date,
  employment_status,
  termination_type,
  uploaded_at,
  ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY effective_date ASC) AS employment_status_sequence, -- need the initiated datetime
  'workday' AS source_system
FROM workday