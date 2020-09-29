WITH bamboohr_compensation_changes AS (

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
    FROM {{ ref('bamboohr_compensation') }}

), pay_frequency AS (

    SELECT *,
     ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) AS pay_frequency_row_number
    FROM {{ ref('bamboohr_job_role') }}
    WHERE pay_frequency IS NOT NULL
  
), ote AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY target_earnings_update_id)   AS rank_ote_effective_date
    FROM {{ ref('bamboohr_ote') }}

), employee_directory AS (

    SELECT *
    FROM {{ ref('employee_directory_intermediate') }}

), currency_conversion AS (

    SELECT *,
      LAG(currency_conversion_factor) OVER (PARTITION BY employee_id ORDER BY conversion_ID)        AS prior_conversion_factor,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY conversion_ID)                           AS rank_conversion_id
    FROM {{ ref('bamboohr_currency_conversion') }}
  
), currency_conversion_factor_periods AS (

    SELECT *,
      LEAD(DATEADD(day,-1,effective_date)) OVER (PARTITION BY employee_id ORDER BY conversion_ID)   AS next_effective_date
    FROM currency_conversion
    WHERE currency_conversion_factor <> prior_conversion_factor
      OR rank_conversion_id = 1

), joined AS (

    SELECT 
      employee_directory.employee_number,
      bamboohr_compensation_changes.*,
      employee_directory.division,
      employee_directory.department,
      employee_directory.job_title,
      {# CASE WHEN pay_rate = 'Month' 
            THEN 12
           WHEN pay_rate = 'Year' 
            THEN 1
           WHEN pay_rate = 'PayPeriod' 
            THEN COALESCE(pay_frequency.pay_frequency, pay_frequency_initial.pay_frequency) 
           ELSE NULL END        AS pay_frequency, -- started capturing pay frequency on 2020.09.10 and are using this frequency for historical data #}
      COALESCE(pay_frequency.pay_frequency, pay_frequency_initial.pay_frequency) AS pay_frequency,     
      currency_conversion_factor,
      ote.variable_pay,
      ote.annual_amount_usd_value AS ote_usd,
      ote.prior_annual_amount_usd AS prior_ote_usd,
      ote.change_in_annual_amount_usd AS ote_change,
      rank_ote_effective_date
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
      division,
      department,
      job_title,
      compensation_change_reason,
      effective_date,
      currency_conversion_factor,
      LAG(currency_conversion_factor) 
        OVER (PARTITION BY employee_id ORDER BY compensation_update_id)                  AS prior_currency_conversion_factor,
      pay_frequency,
      LAG(pay_frequency) OVER (PARTITION BY employee_id ORDER BY compensation_update_id) AS prior_pay_frequency,
      compensation_value                                                                 AS new_compensation_value,
      prior_compensation_value,
      compensation_currency                                                              AS new_compensation_currency,
      prior_compensation_currency,
      variable_pay,
      ote_usd,
      prior_ote_usd,
      ote_change
    FROM joined 
  
), promotions AS (

    SELECT 
      compensation_update_id,
      employee_number,
      employee_id,
      division,
      department,
      job_title,
      effective_date                                                                                  AS promotion_date,
      variable_pay,
      new_compensation_value * pay_frequency * currency_conversion_factor                             AS new_compensation_value_usd,
      CASE WHEN new_compensation_currency = prior_compensation_currency 
           THEN prior_compensation_value * prior_pay_frequency * currency_conversion_factor 
           ELSE prior_compensation_value * prior_pay_frequency * prior_currency_conversion_factor END AS prior_compensation_value_usd,
      new_compensation_value_usd - prior_compensation_value_usd                                       AS change_in_comp_usd,
      COALESCE(ote_change,0) AS ote_change
    FROM intermediate
    WHERE compensation_change_reason = 'Promotion'


)

SELECT 
  promotions.*,
  ote_change+change_in_comp_usd AS total_change
FROM promotions 
 