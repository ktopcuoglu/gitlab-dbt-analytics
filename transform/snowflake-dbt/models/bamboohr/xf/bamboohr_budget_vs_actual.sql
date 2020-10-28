{{ config({
    "schema": "sensitive"
    })
}}

WITH dates AS (

    SELECT *
    FROM {{ ref('date_details') }}

), promotions AS (

    SELECT *
    FROM {{ ref('bamboohr_promotions_xf') }}

), sheetload_people_budget AS (

    SELECT *
    FROM {{ ref('sheetload_people_budget') }}
  
), budget AS (

    SELECT 
      IFF(division='Engineering_Meltano','Engineering',division) AS division,
      fiscal_year,
      fiscal_quarter, 
      budget,
      excess_from_previous_quarter
    FROM sheetload_people_budget

    UNION ALL

    SELECT 
      'Total'                                                   AS division,
      fiscal_year,
      fiscal_quarter, 
      SUM(budget)                                               AS budget,
      SUM(excess_from_previous_quarter)                         AS excess_from_previous_quarter
    FROM sheetload_people_budget
    GROUP BY 1,2,3

), promotions_aggregated AS (

    SELECT
      IFF(division LIKE '%People%' OR division ='CEO',
            'People_CEO',division)                              AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                 AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    GROUP BY 1,2,3

    UNION ALL 

    SELECT
      'Total'                                                  AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    GROUP BY 1,2,3
  
), final AS (

    SELECT 
      budget.*,
      promotions_aggregated.total_spend
    FROM budget
    LEFT JOIN promotions_aggregated
      ON budget.division = promotions_aggregated.division
      AND budget.fiscal_year = promotions_aggregated.fiscal_year
      AND budget.fiscal_quarter = promotions_aggregated.fiscal_quarter

)

SELECT *
FROM final
