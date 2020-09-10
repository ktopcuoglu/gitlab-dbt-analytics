WITH employee_directory AS (

    SELECT *
    FROM {{ ref('employee_directory_intermediate') }}  
    WHERE is_promotion = True

), ote AS (

    SELECT *
    FROM {{ ref('bamboohr_ote_promotions') }}  

), currency_conversion AS (

    SELECT *
    FROM {{ ref('bamboohr_currency_promotions') }}  

), intermediate AS (

    SELECT 
      employee_directory.employee_number,
      employee_directory.employee_id,
      employee_directory.job_title,
      employee_directory.department,
      employee_directory.division,
      employee_directory.date_actual AS promotion_date,
      ote.rank_ote,
      ote.variable_pay,
      ote.ote_usd,
      ote.prior_bamboohr_ote,
      ote.change_in_ote,
      currency_conversion.usd_annual_salary_amount,
      prior_bamboohr_annual_salary,
      currency_conversion.usd_annual_salary_amount - prior_bamboohr_annual_salary AS change_in_annual
    FROM employee_directory
    LEFT JOIN ote
      ON employee_directory.employee_id = ote.employee_id
      AND employee_directory.date_actual = ote.effective_Date
    LEFT JOIN currency_Conversion
      ON employee_directory.employee_id = currency_conversion.employee_id
      AND employee_directory.date_actual = currency_conversion.effective_date
    WHERE job_title NOT LIKE '%VP%'
  
), final aS (

    SELECT *,
      CASE WHEN COALESCE(variable_pay,'No') = 'Yes' AND rank_ote > 1 
            THEN change_in_ote
          WHEN COALESCE(variable_pay,'No') = 'Yes' AND rank_ote = 1 
            THEN ote_usd  - prior_bamboohr_annual_salary
          WHEN COALESCE(variable_pay,'No') = 'No' 
            THEN change_in_annual ELSE NULL END                             AS change_in_comp,
      CASE WHEN COALESCE(variable_pay,'No') = 'Yes' AND rank_ote > 1 
            THEN change_in_ote/prior_bamboohr_ote
          WHEN COALESCE(variable_pay,'No') = 'Yes' AND rank_ote = 1 
            THEN ote_usd  - prior_bamboohr_annual_salary/prior_bamboohr_annual_salary
          WHEN COALESCE(variable_pay,'No') = 'No' 
            THEN change_in_annual/prior_bamboohr_annual_salary ELSE NULL END AS change_in_comp
    FROM intermediate 
    
)

SELECT *
FROM final