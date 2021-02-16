{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH dim_dates as (

    SELECT *
    FROM {{ ref ('dim_date') }}

), rpt AS (

    SELECT *
    FROM {{ ref ('bamboohr_rpt_headcount_aggregation') }}

), rpt_headcount_vs_planned AS (

    SELECT *
    FROM {{ ref ('rpt_headcount_vs_planned') }}
    WHERE breakout_type = 'all_company_breakout' 

), rpt_comp_band AS (

    SELECT *
    FROM {{ ref ('rpt_team_members_out_of_comp_band') }}
    WHERE breakout_type = 'company_breakout' 

), rpt_osat AS (

    SELECT *
    FROM {{ ref ('rpt_osat') }}

), basic_metrics AS (

    SELECT
      'basic_metrics' AS kpi_type,
      dim_dates.fiscal_year,
      dim_dates.fiscal_quarter_name,
      dim_dates.is_first_day_of_last_month_of_fiscal_quarter,
      month_date,
      headcount_end,
      hire_count,
      SUM(hire_count) OVER (PARTITION BY fiscal_year) AS hires_fiscal_year,
      SUM(hire_count)  OVER (ORDER BY month_date
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_month_hires,
      SUM(hire_count) OVER (PARTITION BY fiscal_year ORDER BY month_date rows between unbounded preceding and current row) hires_fiscal_ytd,
      retention,
      voluntary_separation_rate,
      involuntary_separation_rate,
      location_factor,
      discretionary_bonus,
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
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount, NULL)) AS female_headcount,
      SUM(IFF(eeoc_value = 'Female', percent_of_hires, NULL)) AS female_hires,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_manager, NULL)) AS female_managers,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_leaders, NULL)) AS female_leaders,
      SUM(IFF(eeoc_value = 'Female', percent_of_headcount_staff, NULL)) AS female_staff,
      SUM(IFF(eeoc_field_name = 'region_modified' AND eeoc_value != 'NORAM', percent_of_headcount, NULL)) AS non_noram_headcount
      ---% urg group hires
    FROM rpt
    LEFT JOIN dim_dates
      ON dim_dates.date_actual = rpt.month_date
    WHERE breakout_type = 'eeoc_breakout'
    GROUP BY 1,2,3,4

), people_group_metrics AS (

    SELECT
      month_date,
      SUM(IFF(department = 'Recruiting', location_factor, NULL ))       AS recruiting_new_hire_location_factor,
      SUM(IFF(department = 'People Success', location_factor, NULL ))   AS people_success_new_hire_location_factor,
    FROM rpt
    LEFT JOIN dim_dates
      ON dim_dates.date_actual = rpt.month_date
    WHERE breakout_type = 'department_breakout'


), recruiting_metrics AS (



    
)


