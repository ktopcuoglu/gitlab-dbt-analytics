{{ config(alias='report_targets_totals_per_quarter_with_targets') }}

WITH date_details AS (
  
  SELECT *
  FROM   {{ ref('wk_sales_date_details') }} 
  
), report_pipeline_metrics_day AS (

   SELECT *
   FROM  {{ ref('wk_sales_report_pipeline_metrics_per_day') }} 

), report_targets_totals_per_quarter AS (

    SELECT *
    FROM  {{ ref('wk_sales_report_targets_totals_per_quarter') }} 

  
-- make sure the aggregation works at the level we want it
), consolidated_metrics AS (

    SELECT 
        ---------------------------
        -- Keys
        sales_team_cro_level, 
        sales_team_rd_asm_level,
        deal_group,
        sales_qualified_source,
        -----------------------------

        close_fiscal_quarter_date,
        close_fiscal_quarter_name,
        snapshot_fiscal_quarter_date,
        snapshot_fiscal_quarter_name,
        snapshot_fiscal_year,
        snapshot_day_of_fiscal_quarter_normalised,

        -- report quarter plus 1 / 2 date fields
        rq_plus_1_close_fiscal_quarter_name,
        rq_plus_1_close_fiscal_quarter_date,
        rq_plus_2_close_fiscal_quarter_name,
        rq_plus_2_close_fiscal_quarter_date,    

        -- reported quarter
        SUM(deal_count)                 AS deal_count,
        SUM(open_1plus_deal_count)      AS open_1plus_deal_count,
        SUM(open_3plus_deal_count)      AS open_3plus_deal_count,
        SUM(open_4plus_deal_count)      AS open_4plus_deal_count, 
        SUM(booked_deal_count)          AS booked_deal_count,

        SUM(created_in_quarter_count)    AS created_in_quarter_count,

        -- reported quarter + 1
        SUM(rq_plus_1_open_1plus_deal_count)    AS rq_plus_1_open_1plus_deal_count,
        SUM(rq_plus_1_open_3plus_deal_count)    AS rq_plus_1_open_3plus_deal_count,
        SUM(rq_plus_1_open_4plus_deal_count)    AS rq_plus_1_open_4plus_deal_count,

        -- reported quarter + 2
        SUM(rq_plus_2_open_1plus_deal_count)    AS rq_plus_2_open_1plus_deal_count,
        SUM(rq_plus_2_open_3plus_deal_count)    AS rq_plus_2_open_3plus_deal_count,
        SUM(rq_plus_2_open_4plus_deal_count)    AS rq_plus_2_open_4plus_deal_count,

        ------------------------------
        -- Net ARR 
        -- Use Net ARR instead     
        -- created and closed

        -- reported quarter
        SUM(booked_net_arr)                     AS booked_net_arr,
        SUM(open_1plus_net_arr)                 AS open_1plus_net_arr,
        SUM(open_3plus_net_arr)                 AS open_3plus_net_arr, 
        SUM(open_4plus_net_arr)                 AS open_4plus_net_arr, 

        SUM(created_and_won_same_quarter_net_arr)       AS created_and_won_same_quarter_net_arr,
        SUM(created_in_quarter_net_arr)                 AS created_in_quarter_net_arr,

        -- reported quarter + 1
        SUM(rq_plus_1_open_1plus_net_arr)       AS rq_plus_1_open_1plus_net_arr,
        SUM(rq_plus_1_open_3plus_net_arr)       AS rq_plus_1_open_3plus_net_arr,
        SUM(rq_plus_1_open_4plus_net_arr)       AS rq_plus_1_open_4plus_net_arr,

        -- reported quarter + 2
        SUM(rq_plus_2_open_1plus_net_arr)       AS rq_plus_2_open_1plus_net_arr,
        SUM(rq_plus_2_open_3plus_net_arr)       AS rq_plus_2_open_3plus_net_arr,
        SUM(rq_plus_2_open_4plus_net_arr)       AS rq_plus_2_open_4plus_net_arr
    FROM report_pipeline_metrics_day
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
  
), consolidated_targets AS (

    SELECT
        ---------------------------
        -- Keys
        sales_team_cro_level, 
        sales_team_rd_asm_level,
        deal_group,
        sales_qualified_source,
        -----------------------------

        close_fiscal_quarter_name,
        close_fiscal_quarter_date,
     
        close_fiscal_year,
     
        SUM(target_net_arr)                         AS target_net_arr,
        SUM(target_deal_count)                      AS target_deal_count,
        SUM(target_pipe_generation_net_arr)         AS target_pipe_generation_net_arr, 
  
        SUM(total_booked_net_arr)                   AS total_booked_net_arr,
        SUM(total_booked_deal_count)                AS total_booked_deal_count,
        SUM(total_pipe_generation_net_arr)          AS total_pipe_generation_net_arr,
  
        SUM(calculated_target_net_arr)              AS calculated_target_net_arr, 
        SUM(calculated_target_deal_count)           AS calculated_target_deal_count,  
        SUM(calculated_target_pipe_generation)      AS calculated_target_pipe_generation
  FROM report_targets_totals_per_quarter
  GROUP BY 1,2,3,4,5,6,7

  
), key_fields AS (
    
  SELECT         
        sales_team_cro_level, 
        sales_team_rd_asm_level,
        deal_group,
        sales_qualified_source,
        close_fiscal_quarter_date AS snapshot_fiscal_quarter_date
  FROM consolidated_targets
  UNION
  SELECT         
        sales_team_cro_level, 
        sales_team_rd_asm_level,
        deal_group,
        sales_qualified_source,
        snapshot_fiscal_quarter_date
    FROM consolidated_metrics
  
), base_fields AS (
  
 SELECT 
    key_fields.*,
    snap_date.snapshot_day_of_fiscal_quarter_normalised,
    snap_date.snapshot_fiscal_quarter_name
 FROM key_fields
 INNER JOIN (SELECT first_day_of_fiscal_quarter         AS snapshot_fiscal_quarter_date,
                    fiscal_quarter_name_fy              AS snapshot_fiscal_quarter_name,
                    day_of_fiscal_quarter_normalised    AS snapshot_day_of_fiscal_quarter_normalised
             FROM date_details) snap_date
 ON snap_date.snapshot_fiscal_quarter_date = key_fields.snapshot_fiscal_quarter_date
  
), final AS (
  
    SELECT 
        base.sales_team_cro_level, 
        base.sales_team_rd_asm_level,
        base.deal_group,
        base.sales_qualified_source,
  
        base.snapshot_fiscal_quarter_date,
        base.snapshot_fiscal_quarter_name,
        metrics.snapshot_fiscal_year,
        metrics.snapshot_day_of_fiscal_quarter_normalised,

        -- report quarter plus 1 / 2 date fields
        metrics.rq_plus_1_close_fiscal_quarter_name,
        metrics.rq_plus_1_close_fiscal_quarter_date,
        metrics.rq_plus_2_close_fiscal_quarter_name,
        metrics.rq_plus_2_close_fiscal_quarter_date,    

        -- reported quarter
        metrics.deal_count,
        metrics.open_1plus_deal_count,
        metrics.open_3plus_deal_count,
        metrics.open_4plus_deal_count, 
        metrics.booked_deal_count,

        metrics.created_in_quarter_count,

        -- reported quarter + 1
        metrics.rq_plus_1_open_1plus_deal_count,
        metrics.rq_plus_1_open_3plus_deal_count,
        metrics.rq_plus_1_open_4plus_deal_count,

        -- reported quarter + 2
        metrics.rq_plus_2_open_1plus_deal_count,
        metrics.rq_plus_2_open_3plus_deal_count,
        metrics.rq_plus_2_open_4plus_deal_count,

        ------------------------------
        -- Net ARR 
        -- Use Net ARR instead     
        -- created and closed

        -- reported quarter
        metrics.booked_net_arr,
        metrics.open_1plus_net_arr,
        metrics.open_3plus_net_arr, 
        metrics.open_4plus_net_arr, 

        metrics.created_and_won_same_quarter_net_arr,
        metrics.created_in_quarter_net_arr,

        -- reported quarter + 1
        metrics.rq_plus_1_open_1plus_net_arr,
        metrics.rq_plus_1_open_3plus_net_arr,
        metrics.rq_plus_1_open_4plus_net_arr,

        -- reported quarter + 2
        metrics.rq_plus_2_open_1plus_net_arr,
        metrics.rq_plus_2_open_3plus_net_arr,
        metrics.rq_plus_2_open_4plus_net_arr,

        targets.target_net_arr,
        targets.target_deal_count,
        targets.target_pipe_generation_net_arr, 
  
        targets.total_booked_net_arr,
        targets.total_booked_deal_count,
        targets.total_pipe_generation_net_arr,
  
        targets.calculated_target_net_arr, 
        targets.calculated_target_deal_count,  
        targets.calculated_target_pipe_generation
  
    FROM base_fields base 
    LEFT JOIN consolidated_metrics metrics
    ON metrics.sales_team_cro_level = base.sales_team_cro_level
      AND metrics.sales_team_rd_asm_level = base.sales_team_rd_asm_leveL
      AND metrics.sales_qualified_source = base.sales_qualified_source
      AND metrics.deal_group = base.deal_group
      AND metrics.snapshot_day_of_fiscal_quarter_normalised = base.snapshot_day_of_fiscal_quarter_normalised
      AND metrics.snapshot_fiscal_quarter_date = base.snapshot_fiscal_quarter_date
    LEFT JOIN consolidated_targets targets 
    ON metrics.sales_team_cro_level = base.sales_team_cro_level
      AND metrics.sales_team_rd_asm_level = base.sales_team_rd_asm_leveL
      AND metrics.sales_qualified_source = base.sales_qualified_source
      AND metrics.deal_group = base.deal_group
      AND metrics.snapshot_fiscal_quarter_date = base.snapshot_fiscal_quarter_date
 )
 
 SELECT *
 FROM final