{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

{{ simple_cte([
    ('dim_dates','dim_date'),
    ('rpt','bamboohr_rpt_headcount_aggregation'),
    ('rpt_headcount_vs_planned','rpt_headcount_vs_planned'),
    ('rpt_team_members_out_of_comp_band','rpt_team_members_out_of_comp_band'),
    ('rpt_osat','rpt_osat'),
    ('rpt_recruiting_kpis','rpt_recruiting_kpis'),
    ('rpt_cost_per_hire', 'rpt_cost_per_hire'),
    ('rpt_promotion','bamboohr_promotion_rate')

]) }}
 
, basic_metrics AS (

    SELECT
      dim_dates.fiscal_year,
      dim_dates.fiscal_quarter_name,
      dim_dates.is_first_day_of_last_month_of_fiscal_quarter,
      month_date,
      headcount_end,
      hire_count,
      SUM(hire_count) OVER (PARTITION BY fiscal_year) AS hires_fiscal_year,
      SUM(hire_count)  OVER (ORDER BY month_date
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_month_hires,
      SUM(hire_count) OVER (PARTITION BY fiscal_year ORDER BY month_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) hires_fiscal_ytd,
      retention,
      voluntary_separation_rate,
      involuntary_separation_rate,
      location_factor,
      new_hire_location_factor_rolling_3_month,
      discretionary_bonus/headcount_end AS discretionary_bonus_rate,
      rolling_12_month_promotions,
      rolling_12_month_promotions_excluding_sdr,
      rolling_12_month_promotion_increase,
      rolling_12_month_promotion_increase_excluding_sdr      
    FROM rpt
    LEFT JOIN dim_dates
      ON dim_dates.date_actual = rpt.month_date
    WHERE breakout_type = 'kpi_breakout'

), diversity_metrics AS (

    SELECT
      dim_dates.fiscal_year,
      dim_dates.fiscal_quarter_name,
      dim_dates.is_first_day_of_last_month_of_fiscal_quarter,
      month_date,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount, NULL))                        AS female_headcount,
      SUM(IFF(eeoc_value = 'Female', percent_of_hires, NULL))                            AS female_hires,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_manager, NULL))                AS female_managers,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_leaders, NULL))                AS female_leaders,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_staff, NULL))                  AS female_staff,
      
      SUM(IFF(eeoc_field_name = 'region_modified'
              AND eeoc_value != 'NORAM', percent_of_headcount, NULL))                    AS non_noram_headcount,

      SUM(IFF(eeoc_field_name = 'urg_group'
                AND eeoc_value = TRUE, percent_of_hires, NULL))                          AS percent_of_urg_hires,
      SUM(IFF(eeoc_field_name = 'ethnicity'
              AND eeoc_value = 'Black or African American', percent_of_headcount, NULL)) AS percent_of_headcount_black_or_african_american    
    FROM rpt
    LEFT JOIN dim_dates
      ON dim_dates.date_actual = rpt.month_date
    WHERE breakout_type = 'eeoc_breakout'
    GROUP BY 1,2,3,4

), people_group_metrics AS (

    SELECT
      month_date,
      SUM(IFF(department = 'Recruiting', new_hire_location_factor_rolling_3_month, NULL ))       AS recruiting_new_hire_location_factor,
      SUM(IFF(department = 'People Success', new_hire_location_factor_rolling_3_month, NULL ))   AS people_success_new_hire_location_factor
    FROM rpt
    LEFT JOIN dim_dates
      ON dim_dates.date_actual = rpt.month_date
    WHERE breakout_type = 'department_breakout'
      AND eeoc_field_name = 'no_eeoc'
    GROUP BY 1

), greenhouse_metrics AS (

    SELECT *
    FROM rpt_recruiting_kpis

), final AS (

    SELECT
      basic_metrics.*,
      rpt_promotion.promotion_rate                                      AS company_promotion_rate_excluding_sdr,
      sdr_promotion.promotion_rate                                      AS sdr_promotion_rate,
      rpt_headcount_vs_planned.actual_headcount_vs_planned_headcount,
      rpt_cost_per_hire.rolling_3_month_cost_per_hire,
      rpt_team_members_out_of_comp_band.percent_of_employees_outside_of_band,

      people_group_metrics.recruiting_new_hire_location_factor,
      people_group_metrics.people_success_new_hire_location_factor,

      diversity_metrics.female_headcount,
      diversity_metrics.female_hires,
      diversity_metrics.female_managers,
      diversity_metrics.female_leaders,
      diversity_metrics.female_staff,
      diversity_metrics.non_noram_headcount,
      diversity_metrics.percent_of_headcount_black_or_african_american,
      diversity_metrics.percent_of_urg_hires,
  
      rpt_osat.rolling_3_month_osat,
      rpt_osat.rolling_3_month_respondents/basic_metrics.rolling_3_month_hires AS rolling_3_month_osat_response_rate,
      rpt_osat.rolling_3_month_buddy_score,
      rpt_osat.rolling_3_month_buddy_respondents,

      rpt_recruiting_kpis.offer_acceptance_rate,
      rpt_recruiting_kpis.percent_sourced_hires,
      rpt_recruiting_kpis.percent_outbound_hires,
      rpt_recruiting_kpis.time_to_offer_median,
      rpt_recruiting_kpis.isat,
      rpt_headcount_vs_planned.cumulative_hires_vs_plan

    --% urg
    FROM basic_metrics
    LEFT JOIN diversity_metrics
      ON basic_metrics.month_date = diversity_metrics.month_date
    LEFT JOIN people_group_metrics
      ON basic_metrics.month_date = people_group_metrics.month_date
    LEFT JOIN rpt_osat
      ON basic_metrics.month_date = rpt_osat.completed_month
    LEFT JOIN rpt_headcount_vs_planned
      ON basic_metrics.month_date = DATE_TRUNC(month, rpt_headcount_vs_planned.month_date)
      AND rpt_headcount_vs_planned.breakout_type = 'all_company_breakout' 
    LEFT JOIN rpt_recruiting_kpis
      ON basic_metrics.month_date = rpt_recruiting_kpis.month_date
    LEFT JOIN rpt_cost_per_hire
      ON basic_metrics.month_date = rpt_cost_per_hire.hire_month
    LEFT JOIN rpt_promotion
      ON basic_metrics.month_date = rpt_promotion.month_date  
      AND rpt_promotion.field_name = 'company_breakout'
      AND rpt_promotion.field_value = 'Company - Excluding SDR'
    LEFT JOIN rpt_promotion AS sdr_promotion
      ON basic_metrics.month_date = sdr_promotion.month_date  
      AND sdr_promotion.field_name = 'department_grouping_breakout'
      AND sdr_promotion.field_value = 'Sales Development'  
    LEFT JOIN rpt_team_members_out_of_comp_band
      ON basic_metrics.month_date = DATE_TRUNC(month, rpt_team_members_out_of_comp_band.date_actual)  
      AND rpt_team_members_out_of_comp_band.breakout_type = 'company_breakout'
)

SELECT 
  IFF(month_date = DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE())), TRUE, FALSE) AS current_reporting_month,
  IFF(month_date = DATEADD(month, -2, DATE_TRUNC(month, CURRENT_DATE())), TRUE, FALSE) AS previous_reporting_month,
  IFF(month_date = DATEADD(month, -13, DATE_TRUNC(month, CURRENT_DATE())), TRUE, FALSE) AS last_year_reporting_month,
  DENSE_RANK() OVER (ORDER BY fiscal_quarter_name DESC) AS rank_fiscal_quarter_desc,
  final.*
FROM final
WHERE month_date BETWEEN DATEADD(month, -13, DATE_TRUNC(month, CURRENT_DATE()))
                     AND DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE()))
                     
                     