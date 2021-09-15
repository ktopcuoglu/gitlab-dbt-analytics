{{ config(alias='report_pipeline_movement_daily') }}

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }} 

)
, report_pipeline_movement_quarter AS (

  SELECT *
  FROM {{ ref('wk_sales_report_pipeline_movement_quarter') }} 

)
, sfdc_opportunity_snapshot_history_xf AS (

  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  WHERE is_deleted = 0
    AND is_edu_oss = 0

)
, target_day AS (
  
  SELECT 
      day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalised
  FROM date_details 
  WHERE date_actual = CURRENT_DATE

)
,report_period AS (

  SELECT 
    date_actual                       AS report_date,
    day_of_fiscal_quarter_normalised  AS report_day_of_fiscal_quarter_normalised,
    fiscal_quarter_name_fy            AS report_fiscal_quarter_name,
    first_day_of_fiscal_quarter       AS report_fiscal_quarter_date
  FROM date_details
  
)
-- using the daily perspective and the max, min and resolution dates from the quarterly view
-- it is possible to reconstruct a daily changes perspective
, daily_pipeline_changes AS (
  
  SELECT
    report.report_date,
    report.report_day_of_fiscal_quarter_normalised,
    report.report_fiscal_quarter_name,
    report.report_fiscal_quarter_date,
  
    pipe.opportunity_id,
    pipe.min_snapshot_date,
    pipe.max_snapshot_date,
  
    pipe.quarter_start_net_arr,
    pipe.quarter_end_net_arr,
    pipe.last_day_net_arr,
    pipe.pipe_resolution_date,
  
    CASE 
      WHEN report.report_date >= pipe.pipe_resolution_date
        THEN pipe.pipe_resolution
      WHEN report.report_date >= pipe.min_snapshot_date
          AND report.report_date < pipe.pipe_resolution_date
       THEN '7. Open'
      ELSE NULL
    END                                               AS pipe_resolution,
  
    pipe.pipeline_type
  
  FROM report_period report
    INNER JOIN report_pipeline_movement_quarter pipe
      ON  pipe.report_fiscal_quarter_date = report.report_fiscal_quarter_date

), report_pipeline_movemnet_daily AS (
  
  SELECT pipe.*,
    opp_snap.stage_name,
    opp_snap.close_date,
    opp_snap.forecast_category_name,
    -- before the deal was to close in quarter we show null
    -- within start and resolution the net arr of that day
    -- after pipe resolution the value the opty had that last day
    CASE 
      WHEN pipe.report_date >= pipe.pipe_resolution_date
        THEN pipe.last_day_net_arr
      WHEN pipe.report_date >= pipe.min_snapshot_date
          AND pipe.report_date < pipe.pipe_resolution_date
       THEN opp_snap.net_arr
      ELSE NULL
    END                                               AS net_arr
  FROM daily_pipeline_changes pipe
  INNER JOIN sfdc_opportunity_snapshot_history_xf opp_snap
    ON pipe.opportunity_id = opp_snap.opportunity_id
    AND pipe.report_date = opp_snap.snapshot_date
  WHERE pipe.pipe_resolution IS NOT NULL 
 
) 

SELECT *
FROM report_pipeline_movemnet_daily