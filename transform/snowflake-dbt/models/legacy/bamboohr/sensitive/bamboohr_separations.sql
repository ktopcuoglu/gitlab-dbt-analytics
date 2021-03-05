WITH separations AS (

    SELECT
      employee_number,
      employee_id,
      hire_date,
      date_actual                       AS separation_date, 
      DATE_TRUNC(month, date_actual)    AS separated_month, 
      division_mapped_Current as division, department_modified as department, job_title
    FROM {{ ref('employee_directory_intermediate') }}
    WHERE is_termination_date = TRUE
      AND date_Actual>='2020-02-01'

), separation_type AS (

    SELECT *
    FROM {{ ref('bamboohr_employment_status_source') }}
    WHERE employment_status = 'Terminated'

), eeoc AS (

    SELECT *
    FROM {{ ref('bamboohr_id_employee_number_mapping') }}

), final AS (

    SELECT 
      separations.*,
      termination_type,
      eeoc.gender,
      eeoc.ethnicity,
      eeoc.region
    FROM separations
    LEFT JOIN separation_type 
      ON separations.employee_id = separation_type.employee_id
      AND separations.separation_date = separation_type.effective_Date
    LEFT JOIN eeoc 
      ON separations.employee_id = eeoc.employee_id

)

SELECT *
FROM final
