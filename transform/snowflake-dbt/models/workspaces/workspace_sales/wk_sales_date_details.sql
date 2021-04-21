{{ config(alias='date_details') }}

WITH date_details AS (

    SELECT
      *,
      90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS day_of_fiscal_quarter_normalised,
      12-floor((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)/7))  AS week_of_fiscal_quarter_normalised,
      
      --NF: There has to a be a one liner for this...
      CASE 
          WHEN  week_of_fiscal_quarter_normalised < 5
            THEN week_of_fiscal_quarter_normalised 
          WHEN  week_of_fiscal_quarter_normalised < 9 
            THEN week_of_fiscal_quarter_normalised - 4
          ELSE week_of_fiscal_quarter_normalised - 8
      END                                                                   AS week_of_month_normalised,

      -- beggining of the week
      CASE 
        WHEN ((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)-6) % 7 = 0 
                OR date_actual = first_day_of_fiscal_quarter) 
          THEN 1 
          ELSE 0 
      END                                                                   AS is_first_day_of_fiscal_quarter_week_flag,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter)              AS quarter_number 

    FROM {{ ref('date_details') }} 
    ORDER BY 1 DESC

)

SELECT *
FROM date_details