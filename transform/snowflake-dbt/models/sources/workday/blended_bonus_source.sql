WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_custom_bonus_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_bonus_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

unioned AS (
SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.bonus_date,
  bamboohr.bonus_type,
  bamboohr.uploaded_at,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  bonus_date,
  bonus_type,
  uploaded_at,
  'workday' AS source_system
FROM workday
),

filtered AS (

  {{ gitlab_snowflake.workday_bamboohr_blending_filter('unioned', ['employee_id','bonus_date','bonus_type']) }}
  
)

SELECT *
FROM filtered
