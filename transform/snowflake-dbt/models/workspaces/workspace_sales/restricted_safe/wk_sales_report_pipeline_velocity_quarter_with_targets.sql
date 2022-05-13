{{ config(alias='report_pipeline_velocity_quarter_with_targets') }}

-- TODO:
-- NF: Refactor open X metrics to use new fields created in opportunity / snapshot objects


WITH report_pipeline_velocity_quarter AS (
  
  SELECT *
  FROM {{ref('wk_sales_report_pipeline_velocity_quarter')}}  

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }}  

), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter              AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy                   AS current_fiscal_quarter_name,
                   day_of_fiscal_quarter_normalised         AS current_day_of_fiscal_quarter_normalised
   FROM date_details 
   WHERE date_actual = CURRENT_DATE
  
), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 

), sfdc_opportunity_xf AS (
  
  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  CROSS JOIN today_date
  WHERE is_excluded_flag = 0
    AND is_edu_oss = 0
    AND is_deleted = 0

), report_targets_totals_per_quarter AS (
  
  SELECT *
  FROM {{ref('wk_sales_report_targets_totals_per_quarter')}}  

), report_pipeline_velocity AS (
  
  SELECT *
  FROM report_pipeline_velocity_quarter
  CROSS JOIN today_date
   
), consolidated_targets_totals AS (
  
  SELECT
     base.close_fiscal_quarter_name,
     base.close_fiscal_quarter_date,
     -------------------------
     -- keys
     base.report_user_segment_geo_region_area_sqs_ot,
     -------------------------

     base.target_net_arr,
     base.total_churned_contraction_net_arr,
     base.total_churned_contraction_deal_count,
     base.total_booked_net_arr          AS total_net_arr,
     base.calculated_target_net_arr     AS adjusted_target_net_arr
  FROM report_targets_totals_per_quarter base

), pipeline_summary AS (
  
  SELECT 
      pv.close_fiscal_quarter_name,
      pv.close_fiscal_quarter_date,
      pv.close_day_of_fiscal_quarter_normalised,

      -------------------------
      -- keys
      pv.report_user_segment_geo_region_area_sqs_ot,
      -------------------------

      SUM(pv.open_1plus_net_arr)                AS open_1plus_net_arr,
      SUM(pv.open_3plus_net_arr)                AS open_3plus_net_arr,
      SUM(pv.open_4plus_net_arr)                AS open_4plus_net_arr,
      SUM(pv.booked_net_arr)                    AS booked_net_arr,
      SUM(pv.churned_contraction_net_arr)       AS churned_contraction_net_arr
  
  FROM report_pipeline_velocity pv
  WHERE pv.close_fiscal_year >= 2020
     AND (pv.close_day_of_fiscal_quarter_normalised != pv.current_day_of_fiscal_quarter_normalised
          OR pv.close_fiscal_quarter_date != pv.current_fiscal_quarter_date)
  GROUP BY 1, 2,3,4
  UNION
   -- to have the same current values as in X-Ray
  SELECT 
      o.close_fiscal_quarter_name,
      o.close_fiscal_quarter_date,
      o.current_day_of_fiscal_quarter_normalised,

      -------------------------
      -- keys
      o.report_user_segment_geo_region_area_sqs_ot,
      -------------------------     

      SUM(o.open_1plus_net_arr)              AS open_1plus_net_arr,
      SUM(o.open_3plus_net_arr)              AS open_3plus_net_arr,
      SUM(o.open_4plus_net_arr)              AS open_4plus_net_arr,
      SUM(o.booked_net_arr)                  AS booked_net_arr,
      SUM(o.churned_contraction_net_arr)     AS churned_contraction_net_arr
  
  FROM sfdc_opportunity_xf o
  WHERE o.close_fiscal_quarter_name = o.current_fiscal_quarter_name
  GROUP BY 1, 2,3,4

), base_keys AS (


  SELECT 
    pipeline_summary.close_fiscal_quarter_name,
    pipeline_summary.close_fiscal_quarter_date,
    pipeline_summary.close_day_of_fiscal_quarter_normalised,
    pipeline_summary.report_user_segment_geo_region_area_sqs_ot
  FROM pipeline_summary
  UNION
  SELECT 
    consolidated_targets_totals.close_fiscal_quarter_name,
    consolidated_targets_totals.close_fiscal_quarter_date,
    close_day.close_day_of_fiscal_quarter_normalised,
    consolidated_targets_totals.report_user_segment_geo_region_area_sqs_ot
  FROM consolidated_targets_totals
  CROSS JOIN (SELECT DISTINCT close_day_of_fiscal_quarter_normalised
            FROM pipeline_summary) close_day


), pipeline_velocity_with_targets_per_day AS (
  
  SELECT DISTINCT
  
    base.close_fiscal_quarter_name,
    base.close_fiscal_quarter_date,
    base.close_day_of_fiscal_quarter_normalised,

    -------------------------
    -- keys
    base.report_user_segment_geo_region_area_sqs_ot,
    -------------------------

    target.total_churned_contraction_net_arr,
    target.total_churned_contraction_deal_count,

    target.total_net_arr,
    target.target_net_arr,
    target.adjusted_target_net_arr,
  
    ps.open_1plus_net_arr,
    ps.open_3plus_net_arr,
    ps.open_4plus_net_arr,
    ps.booked_net_arr,
    ps.churned_contraction_net_arr
    
  FROM base_keys base
  LEFT JOIN  consolidated_targets_totals target  
    ON target.close_fiscal_quarter_name = base.close_fiscal_quarter_name
    AND target.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
  LEFT JOIN  pipeline_summary ps  
    ON base.close_fiscal_quarter_name = ps.close_fiscal_quarter_name
    AND base.close_day_of_fiscal_quarter_normalised = ps.close_day_of_fiscal_quarter_normalised
    AND base.report_user_segment_geo_region_area_sqs_ot = ps.report_user_segment_geo_region_area_sqs_ot
  -- only consider quarters we have data in the snapshot history
  WHERE base.close_fiscal_quarter_date >= '2019-08-01'::DATE
  AND base.close_day_of_fiscal_quarter_normalised <= 90

), final AS (

  SELECT 
    agg.*,

    agg_demo_keys.report_opportunity_user_segment,     
    agg_demo_keys.report_opportunity_user_geo,
    agg_demo_keys.report_opportunity_user_region,    
    agg_demo_keys.report_opportunity_user_area,  

    agg_demo_keys.sales_team_cro_level,
    agg_demo_keys.sales_team_vp_level,
    agg_demo_keys.sales_team_avp_rd_level,
    agg_demo_keys.sales_team_asm_level,
    agg_demo_keys.deal_category,
    agg_demo_keys.deal_group,
    agg_demo_keys.sales_qualified_source,
    agg_demo_keys.sales_team_rd_asm_level,

    agg_demo_keys.key_sqs,
    agg_demo_keys.key_ot,

    agg_demo_keys.key_segment,
    agg_demo_keys.key_segment_sqs,                 
    agg_demo_keys.key_segment_ot,   

    agg_demo_keys.key_segment_geo,
    agg_demo_keys.key_segment_geo_sqs,
    agg_demo_keys.key_segment_geo_ot,      

    agg_demo_keys.key_segment_geo_region,
    agg_demo_keys.key_segment_geo_region_sqs,
    agg_demo_keys.key_segment_geo_region_ot,   

    agg_demo_keys.key_segment_geo_region_area,
    agg_demo_keys.key_segment_geo_region_area_sqs,
    agg_demo_keys.key_segment_geo_region_area_ot,

    agg_demo_keys.key_segment_geo_area,

    agg_demo_keys.report_user_segment_geo_region_area

  FROM pipeline_velocity_with_targets_per_day agg
  LEFT JOIN agg_demo_keys
    ON agg.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot


)

SELECT *
FROM final