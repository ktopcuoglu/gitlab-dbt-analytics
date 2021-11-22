WITH hires AS (
    
    SELECT *
    FROM {{ ref('greenhouse_hires') }} 
)

SELECT
  hires.unique_key,
  hires.hire_date_mod               AS hire_date,
  hires.region,
  hires.division,
  hires.department,
  hires.hired_in_bamboohr,
  hires.candidate_id,
  hires.job_opening_type,
  hires.hire_type
FROM hires

