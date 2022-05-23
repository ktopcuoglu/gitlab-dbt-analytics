WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_emergency_contacts_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_emergency_contacts_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
)

SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.full_name,
  bamboohr.home_phone,
  bamboohr.mobile_phone,
  bamboohr.work_phone,
  bamboohr.uploaded_at,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  full_name,
  home_phone,
  mobile_phone,
  work_phone,
  uploaded_at,
  'workday' AS source_system
FROM workday