{{ config({
    "schema": "ephemeral"
    })
}}

WITH dates AS (

    SELECT *,
      'join' AS join_field
    FROM  {{ ref('dim_dates') }}
    WHERE date_actual BETWEEN DATEADD(YEAR,-1, DATEADD(month,-1,DATE_TRUNC(month, CURRENT_DATE())))
                          AND DATEADD(month,-1,DATE_TRUNC(month, CURRENT_DATE()))
      AND day_of_month = 1

), division_department_mapping AS (

    SELECT *,
      'join' AS join_field
    FROM  {{ ref('cost_center_division_department_mapping_current') }}  

), unioned AS (

    SELECT 
      DISTINCT 
      dates.date_actual,
      'division' AS field_name,
      division   AS field_value
    FROM dates
    LEFT JOIN division_department_mapping
      ON dates.join_field = division_department_mapping.join_field
    
    UNION ALL

    SELECT 
      DISTINCT 
      dates.date_actual,
      'department' AS field_name,
      department   AS field_value
    FROM dates
    LEFT JOIN division_department_mapping
      ON dates.join_field = division_department_mapping.join_field

)

SELECT *
FROM unioned