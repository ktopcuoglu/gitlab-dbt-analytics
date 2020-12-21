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
    FROM {{ ref('dim_date') }}
    WHERE (last_day_of_month < '2020-05-20' --last day we captured before transitioning to new report
      OR last_day_of_month>='2020-10-31') -- started capturing again from new report
      AND last_day_of_month<= CURRENT_DATE()

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
        ELSE NULL END                                      AS  weighted_deviated_from_comp_calc
    FROM employee_directory_intermediate
    LEFT JOIN comp_band
        ON employee_directory_intermediate.employee_number = comp_band.employee_number
        AND valid_from <= date_actual
        AND COALESCE(valid_to::date, {{max_date_in_bamboo_analyses()}}) > date_actual

), department_aggregated AS (

    SELECT
      'department_breakout'                 AS breakout_type,
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

), division_aggregated AS (

    SELECT
      'division_breakout'                   AS breakout_type,
      date_actual,
      division,
      'division_breakout'                   AS department,
      SUM(weighted_deviated_from_comp_calc) AS sum_weighted_deviated_from_comp_calc,
      COUNT(distinct employee_number)       AS current_employees,
      sum_weighted_deviated_from_comp_calc/
        current_employees                   AS percent_of_employees_outside_of_band
    FROM joined
    WHERE date_actual < CURRENT_DATE
    {{ dbt_utils.group_by(n=4) }}

), company_aggregated AS (

    SELECT
      'company_breakout'                    AS breakout_type,
      date_actual,
      'Company - Overall'                   AS division,
      'company_breakout'                    AS department,
      SUM(weighted_deviated_from_comp_calc) AS sum_weighted_deviated_from_comp_calc,
      COUNT(distinct employee_number)       AS current_employees,
      sum_weighted_deviated_from_comp_calc/
        current_employees                   AS percent_of_employees_outside_of_band
    FROM joined
    WHERE date_actual < CURRENT_DATE
    {{ dbt_utils.group_by(n=4) }}

), unioned AS (

    SELECT * 
    FROM department_aggregated

    UNION ALL

    SELECT *
    FROM division_aggregated

    UNION ALL

    SELECT * 
    FROM company_aggregated

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['date_actual', 'breakout_type', 'division', 'department']) }} AS unique_key,
      unioned.*
    FROM unioned
    INNER JOIN date_details
      ON unioned.date_actual = date_details.last_day_of_month
    WHERE date_actual > '2019-01-01'

)
SELECT *
FROM final
ORDER BY 1
