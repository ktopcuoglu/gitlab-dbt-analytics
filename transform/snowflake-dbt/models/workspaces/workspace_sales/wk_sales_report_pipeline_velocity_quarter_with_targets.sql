{{ config(alias='report_pipeline_velocity_quarter_with_targets') }}

-- TODO:
-- NF: Refactor open X metrics to use new fields created in opportunity / snapshot objects


WITH report_pipeline_velocity_quarter AS (
  
  SELECT *
  FROM {{ref('wk_sales_report_pipeline_velocity_quarter')}}  
  WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }}  

), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter              AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy                   AS current_fiscal_quarter_name,
                   day_of_fiscal_quarter_normalised         AS current_day_of_fiscal_quarter_normalised
   FROM date_details 
   WHERE date_actual = CURRENT_DATE
  
), sfdc_opportunity_xf AS (
  
  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  CROSS JOIN today_date
  WHERE is_excluded_flag = 0
    AND is_edu_oss = 0
    AND is_deleted = 0
    AND LOWER(deal_group) LIKE ANY ('%growth%','%new%')

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
    WHERE is_deleted = 0
      AND is_edu_oss = 0

), report_targets_totals_per_quarter AS (
  
  SELECT *
  FROM {{ref('wk_sales_report_targets_totals_per_quarter')}}  

), report_pipeline_velocity AS (
  
  SELECT *
  FROM report_pipeline_velocity_quarter
  CROSS JOIN today_date
  WHERE is_excluded_flag = 0
    AND LOWER(deal_group) LIKE ANY ('%growth%','%new%')
   
), consolidated_targets_totals AS (
  
  SELECT
     base.close_fiscal_quarter_name,
     base.close_fiscal_quarter_date,
     base.sales_team_rd_asm_level,
     base.sales_team_cro_level,
     base.sales_qualified_source,
     base.deal_group,
     base.target_net_arr,
     base.total_churned_net_arr,
     base.total_churned_deal_count,
     base.total_booked_net_arr          AS total_net_arr,
     base.calculated_target_net_arr     AS adjusted_target_net_arr
  FROM report_targets_totals_per_quarter base

), pipeline_summary AS (
  
  SELECT 
      pv.close_fiscal_quarter_name,
      pv.close_fiscal_quarter_date,
      pv.close_day_of_fiscal_quarter_normalised,

      COALESCE(pv.sales_team_rd_asm_level,'NA') AS sales_team_rd_asm_level,
      COALESCE(pv.sales_team_cro_level,'NA')    AS sales_team_cro_level,
      COALESCE(pv.sales_qualified_source,'NA')  AS sales_qualified_source,
      COALESCE(pv.deal_group,'NA')              AS deal_group,

      SUM(pv.open_1plus_net_arr)        AS open_stage_1_net_arr,
      SUM(pv.open_3plus_net_arr)        AS open_stage_3_net_arr,
      SUM(pv.open_4plus_net_arr)        AS open_stage_4_net_arr,
      SUM(pv.booked_net_arr)            AS won_net_arr,
      SUM(pv.churned_net_arr)           AS churned_net_arr
  
  FROM report_pipeline_velocity pv
  WHERE pv.close_fiscal_year >= 2020
     AND (pv.close_day_of_fiscal_quarter_normalised != pv.current_day_of_fiscal_quarter_normalised
          OR pv.close_fiscal_quarter_date != pv.current_fiscal_quarter_date)
  GROUP BY 1, 2,3,4,5,6,7
  UNION
   -- to have the same current values as in X-Ray
  SELECT 
      o.close_fiscal_quarter_name,
      o.close_fiscal_quarter_date,
      o.current_day_of_fiscal_quarter_normalised,

      o.sales_team_rd_asm_level,
      o.sales_team_cro_level,
      o.sales_qualified_source,
      o.deal_group,

      SUM(o.open_1plus_net_arr)              AS open_stage_1_net_arr,
      SUM(o.open_3plus_net_arr)              AS open_stage_3_net_arr,
      SUM(o.open_4plus_net_arr)              AS open_stage_4_net_arr,
      SUM(o.booked_net_arr)                  AS won_net_arr,
      SUM(o.churned_net_arr)                 AS churned_net_arr
  
  FROM sfdc_opportunity_xf o
  WHERE o.close_fiscal_quarter_name = o.current_fiscal_quarter_name
  GROUP BY 1, 2,3,4,5,6,7

), pipeline_velocity_with_targets_per_day AS (
  
  SELECT
  
    base.close_fiscal_quarter_name,
    base.close_fiscal_quarter_date,
    base.close_day_of_fiscal_quarter_normalised,

    base.sales_team_rd_asm_level,
    base.sales_team_cro_level,
    base.sales_qualified_source,
    base.deal_group,
  
    target.total_churned_net_arr,
    target.total_churned_deal_count,

    target.total_net_arr,
    target.target_net_arr,
    target.adjusted_target_net_arr,
  
    ps.open_stage_1_net_arr,
    ps.open_stage_3_net_arr,
    ps.open_stage_4_net_arr,
    ps.won_net_arr,
    ps.churned_net_arr
    
  FROM (
     SELECT close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        close_day_of_fiscal_quarter_normalised,
        sales_team_rd_asm_level,
        sales_team_cro_level,
        sales_qualified_source,
        deal_group
      FROM pipeline_summary
      UNION
      SELECT close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        close_day_of_fiscal_quarter_normalised,
        sales_team_rd_asm_level,
        sales_team_cro_level,
        sales_qualified_source,
        deal_group
      FROM consolidated_targets_totals
      CROSS JOIN (SELECT DISTINCT close_day_of_fiscal_quarter_normalised
                FROM pipeline_summary) close_day) base
  LEFT JOIN  consolidated_targets_totals target  
    ON target.close_fiscal_quarter_name = base.close_fiscal_quarter_name
    AND target.sales_team_rd_asm_level = base.sales_team_rd_asm_level
    AND target.sales_team_cro_level = base.sales_team_cro_level
    AND target.sales_qualified_source = base.sales_qualified_source
    AND target.deal_group = base.deal_group
  LEFT JOIN  pipeline_summary ps  
    ON base.close_fiscal_quarter_name = ps.close_fiscal_quarter_name
    AND base.sales_team_rd_asm_level = ps.sales_team_rd_asm_level
    AND base.sales_team_cro_level = ps.sales_team_cro_level
    AND base.sales_qualified_source = ps.sales_qualified_source
    AND base.deal_group = ps.deal_group
    AND base.close_day_of_fiscal_quarter_normalised = ps.close_day_of_fiscal_quarter_normalised
  -- only consider quarters we have data in the snapshot history
  WHERE base.close_fiscal_quarter_date >= '2019-08-01'::DATE
  AND base.close_day_of_fiscal_quarter_normalised <= 90
)

SELECT *
FROM pipeline_velocity_with_targets_per_day