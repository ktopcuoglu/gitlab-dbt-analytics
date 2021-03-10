WITH bamboohr_compensation AS (

    SELECT *
    FROM {{ ref('bamboohr_compensation_source') }}

), bamboohr_compensation_changes AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) AS rank_by_effective_date,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY compensation_update_id)                             AS rank_by_id,
      CASE WHEN rank_by_effective_date != rank_by_id
           THEN LAG(compensation_value) OVER (PARTITION BY employee_id ORDER BY effective_date)
           ELSE LAG(compensation_value) OVER (PARTITION BY employee_id ORDER BY compensation_update_id) END    AS prior_compensation_value,
      CASE WHEN rank_by_effective_date != rank_by_id
           THEN LAG(compensation_currency) OVER (PARTITION BY employee_id ORDER BY effective_date)
           ELSE LAG(compensation_currency) OVER (PARTITION BY employee_id ORDER BY compensation_update_id) END AS prior_compensation_currency,
      ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY compensation_update_id)             AS rank_compensation_change_effective_date    
    FROM bamboohr_compensation

), pay_frequency AS (

    SELECT *,
     ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) AS pay_frequency_row_number
    FROM {{ ref('bamboohr_job_role') }}
    WHERE pay_frequency IS NOT NULL
  
), ote AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY target_earnings_update_id)   AS rank_ote_effective_date
    FROM {{ ref('bamboohr_ote_source') }}

), employee_directory AS (

    SELECT *
    FROM {{ ref('employee_directory_intermediate') }}

), currency_conversion AS (

    SELECT *,
      LAG(currency_conversion_factor) OVER (PARTITION BY employee_id ORDER BY conversion_ID)        AS prior_conversion_factor,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY conversion_ID)                           AS rank_conversion_id
    FROM {{ ref('bamboohr_currency_conversion_source') }}
  
), currency_conversion_factor_periods AS (

    SELECT *,
      LEAD(annual_amount_usd_value) OVER (PARTITION BY employee_id ORDER BY conversion_ID)          AS next_usd_value,
      LEAD(DATEADD(day,-1,effective_date)) OVER (PARTITION BY employee_id ORDER BY conversion_ID)   AS next_effective_date
    FROM currency_conversion
    WHERE currency_conversion_factor <> prior_conversion_factor
      OR rank_conversion_id = 1

), joined AS (

    SELECT 
      employee_directory.employee_number,
      employee_directory.full_name,
      bamboohr_compensation_changes.*,
      employee_directory.division_mapped_current                                    AS division,
      employee_directory.division_grouping,
      employee_directory.department_modified                                        AS department,
      employee_directory.department_grouping,
      employee_directory.job_title,
      CASE 
        WHEN bamboohr_compensation_changes.employee_id IN (40955, 40647, 41234, 40985, 
                                                   41027, 40782, 40540) 
            AND bamboohr_compensation_changes.effective_date <='2020-06-01' 
          THEN 12
        --we didn't capture pay frequency prior to 2020.07 and in 2020.07 the pay frequency had changed for these individuals  
        WHEN bamboohr_compensation_changes.employee_id ='40874' AND bamboohr_compensation_changes.effective_date < '2019-12-31' 
          THEN 12
        --This team member has a pay frequency of 12 prior to the 2019.12.31 and the current pay frequency for 2020
        ELSE COALESCE(pay_frequency.pay_frequency, pay_frequency_initial.pay_frequency) END AS pay_frequency,    
      currency_conversion_factor,
      ote.variable_pay,
      ote.annual_amount_usd_value AS ote_usd,
      ote.prior_annual_amount_usd AS prior_ote_usd,
      ote.change_in_annual_amount_usd AS ote_change,
      rank_ote_effective_date,
      currency_conversion_factor_periods.annual_amount_usd_value,
      currency_conversion_factor_periods.next_usd_value
    FROM bamboohr_compensation_changes
    LEFT JOIN employee_directory
      ON bamboohr_compensation_changes.employee_id = employee_directory.employee_id
      AND bamboohr_compensation_changes.effective_date = employee_directory.date_actual
    LEFT JOIN pay_frequency
      ON bamboohr_compensation_changes.employee_id = pay_frequency.employee_id
      AND bamboohr_compensation_changes.effective_date BETWEEN pay_frequency.effective_date                                                        AND pay_frequency.next_effective_date
    LEFT JOIN pay_frequency AS pay_frequency_initial 
      ON bamboohr_compensation_changes.employee_id = pay_frequency_initial.employee_id
      AND pay_frequency_initial.pay_frequency_row_number = 1 
    LEFT JOIN currency_conversion_factor_periods
      ON bamboohr_compensation_changes.employee_id = currency_conversion_factor_periods.employee_id
      AND bamboohr_compensation_changes.effective_date BETWEEN currency_conversion_factor_periods.effective_date                                                            AND COALESCE(currency_conversion_factor_periods.next_effective_date, CURRENT_DATE())
    LEFT JOIN ote
      ON bamboohr_compensation_changes.employee_id = ote.employee_id
      AND bamboohr_compensation_changes.effective_date = ote.effective_date
      AND bamboohr_compensation_changes.rank_compensation_change_effective_date = ote.rank_ote_effective_date
   
), intermediate AS (

    SELECT 
      compensation_update_id,
      employee_number,
      employee_id,
      full_name,
      division,
      division_grouping,
      department,
      department_grouping,
      job_title,
      compensation_change_reason,
      effective_date,
      currency_conversion_factor,
      LAG(currency_conversion_factor) 
        OVER (PARTITION BY employee_id ORDER BY compensation_update_id)                  AS prior_currency_conversion_factor,
      pay_frequency,
      LAG(pay_frequency) OVER (PARTITION BY employee_id ORDER BY compensation_update_id) AS prior_pay_frequency,
      compensation_value                                                                 AS new_compensation_value,
      prior_compensation_value                                                           AS prior_compensation_value,
      compensation_currency                                                              AS new_compensation_currency,
      prior_compensation_currency,
      variable_pay,
      ote_usd,
      prior_ote_usd,
      ote_change,
      next_usd_value,
      annual_amount_usd_value
    FROM joined 
  
), promotions AS (

    SELECT 
      compensation_update_id,
      effective_date                                                                                  AS promotion_date,
      DATE_TRUNC(month, effective_date)                                                               AS promotion_month,
      employee_number,
      employee_id,
      full_name,
      division,
      division_grouping,
      department,
      department_grouping,
      job_title,
      variable_pay,
      IFF(compensation_update_id = 21917, next_usd_value, 
            new_compensation_value * pay_frequency * currency_conversion_factor)                      AS new_compensation_value_usd,
      CASE 
        WHEN compensation_update_id = 21917
          THEN annual_amount_usd_value
        WHEN new_compensation_currency = prior_compensation_currency 
           THEN prior_compensation_value * prior_pay_frequency * currency_conversion_factor 
           ELSE prior_compensation_value * prior_pay_frequency * prior_currency_conversion_factor END AS prior_compensation_value_usd,
      new_compensation_value_usd - prior_compensation_value_usd                                       AS change_in_comp_usd,
      COALESCE(ote_usd,0)                                                                             AS ote_usd,
      COALESCE(prior_ote_usd,0)                                                                       AS prior_ote_usd,
      COALESCE(ote_change,0)                                                                          AS ote_change,
      IFF(compensation_update_id = 20263, NULL,  ---incorrectly labeled 
        COALESCE(ote_change,0) + change_in_comp_usd)                                                  AS total_change_in_comp,
      IFF(compensation_update_id = 20263, NULL,
        ROUND((COALESCE(ote_change,0) + change_in_comp_usd)/
        (prior_compensation_value_usd+ COALESCE(prior_ote_usd,0)),2))                                 AS percent_change_in_comp
    FROM intermediate
    WHERE compensation_change_reason = 'Promotion'
      AND job_title NOT LIKE '%VP%'

)

SELECT *
FROM promotions 
