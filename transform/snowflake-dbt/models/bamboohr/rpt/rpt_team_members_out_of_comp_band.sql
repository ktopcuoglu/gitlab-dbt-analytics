{{ config({
    "schema": "legacy"
    })
}}

WITH employee_directory_intermediate AS (

    SELECT *
    FROM {{ref('employee_directory_intermediate')}}

 ), comp_band AS (

    SELECT *
    FROM {{ ref('comp_band_deviation_snapshots') }}

 ), date_details AS (

    SELECT DISTINCT last_day_of_month
    FROM {{ref('date_details')}}

), joined AS (

    SELECT 
      employee_directory_intermediate.*,
      comp_band.deviation_from_comp_calc,
      comp_band.original_value,
      CASE 
        WHEN LOWER(original_value) = 'exec'           THEN 0
        WHEN deviation_from_comp_calc <= 0.0001       THEN 0
        WHEN deviation_from_comp_calc <= 0.05         THEN 0.25
        WHEN deviation_from_comp_calc <= 0.1          THEN 0.5
        WHEN deviation_from_comp_calc <= 0.15         THEN 0.75
        WHEN deviation_from_comp_calc <= 1            THEN 1
        ELSE 1 END                                      AS  weighted_deviated_from_comp_calc
    FROM employee_directory_intermediate
    LEFT JOIN comp_band
        ON employee_directory_intermediate.employee_number = comp_band.employee_number
        AND valid_from <= date_actual
        AND COALESCE(valid_to::date, {{max_date_in_bamboo_analyses()}}) > date_actual

), bucketed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['date_actual', 'division', 'department']) }} AS unique_key,
      date_actual,
      division,
      department,
      SUM(weighted_deviated_from_comp_calc) AS sum_weighted_deviated_from_comp_calc,
      COUNT(distinct employee_number)       AS current_employees,
      sum_weighted_deviated_from_comp_calc/
        current_employees                   AS percent_of_employees_outside_of_band
    FROM joined
    WHERE date_actual < CURRENT_DATE
    {{ dbt_utils.group_by(n=4) }}

), final AS (

    SELECT *
    FROM bucketed
    INNER JOIN date_details
      ON bucketed.date_actual = date_details.last_day_of_month
    WHERE date_actual > '2019-01-01'

)

SELECT *
FROM final
ORDER BY 1
