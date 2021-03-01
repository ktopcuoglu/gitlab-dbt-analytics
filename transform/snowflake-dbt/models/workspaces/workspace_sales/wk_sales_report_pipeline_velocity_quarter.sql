{{ config(alias='report_pipeline_velocity_quarter') }}
WITH date_details AS (

    SELECT
      *,
      90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS day_of_fiscal_quarter_normalised,
      12-floor((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)/7))  AS week_of_fiscal_quarter_normalised,
      CASE 
        WHEN ((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)-6) % 7 = 0 
                OR date_actual = first_day_of_fiscal_quarter) 
          THEN 1 
          ELSE 0 
      END                                                                   AS first_day_of_fiscal_quarter_week_normalised 
    FROM {{ ref('date_details') }} 
    ORDER BY 1 DESC

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  
), report_pipeline_velocity_quarter AS (

    SELECT
      snapshot_date,
      snapshot_fiscal_quarter_name,
      snapshot_fiscal_quarter_date,
      snapshot_fiscal_year,
      snapshot_day_of_fiscal_quarter_normalised,
      close_fiscal_quarter_name,
      close_fiscal_quarter_date,
      close_fiscal_year,
      sales_team_cro_level,
      sales_team_rd_asm_level,
      order_type_stamped,
      stage_name_3plus,
      stage_name_4plus,
      is_excluded_flag,
      stage_name,
      forecast_category_name,
      SUM(calculated_deal_count)                              AS opps,
      SUM(net_arr)                                            AS net_arr,
      SUM(net_incremental_acv)                                AS net_iacv,
      SUM(incremental_acv)                                    AS incremental_acv,
      SUM(total_contract_value)                               AS tcv
    FROM sfdc_opportunity_snapshot_history_xf 
    WHERE 
      -- 2 quarters before start and full quarter, total rolling 9 months at end of quarter
      -- till end of quarter
      snapshot_date <= DATEADD(month,3,close_fiscal_quarter_date)
      -- 2 quarters before start
      AND snapshot_date >= DATEADD(month,-6,close_fiscal_quarter_date)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12, 13, 14, 15, 16

)

SELECT *
FROM report_pipeline_velocity_quarter
