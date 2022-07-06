{{ config(alias='report_pipeline_fy_metrics_per_day_with_targets') }}

WITH date_details AS (
  
  SELECT *
  FROM   {{ ref('wk_sales_date_details') }} 
  
), report_pipeline_fy_metrics_day AS (

   SELECT *
   FROM  {{ ref('wk_sales_report_pipeline_fy_metrics_per_day') }} 

), report_targets_totals_per_quarter AS (

    SELECT *
    FROM  {{ ref('wk_sales_report_targets_totals_per_quarter') }} 
  
), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 


), today_date AS (

    SELECT DISTINCT fiscal_year                         AS current_fiscal_fiscal_year,
                    day_of_fiscal_year_normalised    AS current_day_of_fiscal_year_normalised
    FROM date_details 
    WHERE date_actual = CURRENT_DATE

-- make sure the aggregation works at the level we want it
), consolidated_metrics AS (

    SELECT 
        ---------------------------
        -- Keys
        report_user_segment_geo_region_area_sqs_ot,
        -----------------------------

        close_fiscal_year,
        close_day_of_fiscal_year_normalised,

        SUM(cfy_deal_count)                           AS cfy_deal_count,
        SUM(cfy_booked_deal_count)                    AS cfy_booked_deal_count,
        SUM(cfy_churned_contraction_deal_count)       AS cfy_churned_contraction_deal_count,

        SUM(cfy_open_1plus_deal_count)                AS cfy_open_1plus_deal_count,
        SUM(cfy_open_3plus_deal_count)                AS cfy_open_3plus_deal_count,
        SUM(cfy_open_4plus_deal_count)                AS cfy_open_4plus_deal_count,

        -----------------------------------------------------------------------------------
        -- NF: 20210201  NET ARR fields
        
        SUM(cfy_open_1plus_net_arr)                   AS cfy_open_1plus_net_arr,
        SUM(cfy_open_3plus_net_arr)                   AS cfy_open_3plus_net_arr,
        SUM(cfy_open_4plus_net_arr)                   AS cfy_open_4plus_net_arr,
        SUM(cfy_booked_net_arr)                       AS cfy_booked_net_arr,

      -- pipe gen
        SUM(cfy_pipe_gen_count)                       AS cfy_pipe_gen_count,
        SUM(cfy_pipe_gen_net_arr)                     AS cfy_pipe_gen_net_arr,

        -- sao gen
        SUM(cfy_sao_deal_count)                   AS cfy_sao_deal_count,
        SUM(cfy_sao_net_arr)                          AS cfy_sao_net_arr,

        -- churned net_arr
        SUM(cfy_churned_contraction_net_arr)          AS cfy_churned_contraction_net_arr

    FROM report_pipeline_fy_metrics_day
    WHERE close_day_of_fiscal_year_normalised > 0
    GROUP BY 1,2,3

), fy_targets AS (

  SELECT 
        ---------------------------
        -- Keys
        report_user_segment_geo_region_area_sqs_ot,
        close_fiscal_year,
        -----------------------------


        SUM(cfy_deal_count)                           AS total_fy_deal_count,
        SUM(cfy_booked_deal_count)                    AS total_fy_booked_deal_count,
        SUM(cfy_churned_contraction_deal_count)       AS total_fy_churned_contraction_deal_count,

        SUM(cfy_booked_net_arr)                       AS total_fy_booked_net_arr,
        SUM(cfy_churned_contraction_net_arr)          AS total_fy_churned_contraction_net_arr,

        -- pipe gen
        SUM(cfy_pipe_gen_count)                       AS total_fy_pipe_gen_count,
        SUM(cfy_pipe_gen_net_arr)                     AS total_fy_pipe_gen_net_arr,

        -- sao gen
        SUM(cfy_sao_deal_count)                       AS total_fy_sao_deal_count,
        SUM(cfy_sao_net_arr)                          AS total_fy_sao_net_arr

  FROM consolidated_metrics
  WHERE close_day_of_fiscal_year_normalised = 365
  GROUP BY 1, 2

), source_targets AS (

    SELECT
        ---------------------------
        -- Keys
        report_user_segment_geo_region_area_sqs_ot,
        -----------------------------

        close_fiscal_year,
     
        SUM(target_net_arr)                         AS target_net_arr,
        SUM(target_deal_count)                      AS target_deal_count,
        SUM(target_pipe_generation_net_arr)         AS target_pipe_generation_net_arr

  FROM report_targets_totals_per_quarter
  GROUP BY 1,2

-- combine the yeaar from actuals and from targets to get the table key base
), targets_key AS (

  SELECT DISTINCT 
    close_fiscal_year,
    report_user_segment_geo_region_area_sqs_ot
  FROM fy_targets
  UNION
  SELECT DISTINCT 
    close_fiscal_year,
    report_user_segment_geo_region_area_sqs_ot
  FROM source_targets


), consolidated_targets AS (

        
      SELECT
        targets_key.close_fiscal_year,
        targets_key.report_user_segment_geo_region_area_sqs_ot,

        -----------------------------------------------------------
        source_targets.target_net_arr,
        source_targets.target_deal_count,
        source_targets.target_pipe_generation_net_arr,

        fy_targets.total_fy_deal_count,
        fy_targets.total_fy_booked_deal_count,
        fy_targets.total_fy_churned_contraction_deal_count,

        fy_targets.total_fy_booked_net_arr,
        fy_targets.total_fy_churned_contraction_net_arr,

        -- pipe gen
        fy_targets.total_fy_pipe_gen_count,
        fy_targets.total_fy_pipe_gen_net_arr,

        -- sao gen
        fy_targets.total_fy_sao_deal_count,
        fy_targets.total_fy_sao_net_arr,

        -- check if we are in the current fiscal year or not. If not, use total, if we are use target
        CASE
          WHEN today_date.current_fiscal_fiscal_year <= source_targets.close_fiscal_year
            THEN source_targets.target_net_arr
          ELSE fy_targets.total_fy_booked_net_arr
        END                                         AS calculated_target_net_arr, 
          CASE
          WHEN today_date.current_fiscal_fiscal_year <= source_targets.close_fiscal_year
            THEN source_targets.target_deal_count
          ELSE fy_targets.total_fy_booked_deal_count
        END                                         AS calculated_target_deal_count, 
        CASE
          WHEN today_date.current_fiscal_fiscal_year <= source_targets.close_fiscal_year
            THEN source_targets.target_pipe_generation_net_arr
          ELSE fy_targets.total_fy_pipe_gen_net_arr
        END                                         AS calculated_target_pipe_generation

      FROM targets_key
      CROSS JOIN today_date 
      LEFT JOIN source_targets
        ON targets_key.report_user_segment_geo_region_area_sqs_ot = source_targets.report_user_segment_geo_region_area_sqs_ot
        AND targets_key.close_fiscal_year = source_targets.close_fiscal_year
      LEFT JOIN fy_targets
        ON targets_key.report_user_segment_geo_region_area_sqs_ot = fy_targets.report_user_segment_geo_region_area_sqs_ot
        AND targets_key.close_fiscal_year = fy_targets.close_fiscal_year

), consolidated_targets_per_day AS (
  
  SELECT 
        targets.*,
        close_day_of_fiscal_year_normalised
  FROM consolidated_targets targets
  CROSS JOIN (SELECT day_of_fiscal_year_normalised AS close_day_of_fiscal_year_normalised
                FROM date_details
                WHERE day_of_fiscal_year_normalised > 0
                GROUP BY 1)

), key_fields AS (
    
  SELECT         
      report_user_segment_geo_region_area_sqs_ot,
      close_fiscal_year
  FROM consolidated_targets
  UNION
  SELECT         
      report_user_segment_geo_region_area_sqs_ot,
      close_fiscal_year
  FROM consolidated_metrics
  
), base_fields AS (
  
 SELECT 
    key_fields.*,
    close_year.day_of_fiscal_year_normalised       AS close_day_of_fiscal_year_normalised
 FROM key_fields
 INNER JOIN date_details close_year
    ON close_year.fiscal_year = key_fields.close_fiscal_year 
  
), final AS (
  
    SELECT 

        --------------------------
        -- keys
        base.report_user_segment_geo_region_area_sqs_ot,
        --------------------------

        base.close_fiscal_year,
        base.close_day_of_fiscal_year_normalised,

        ----------------------------------------

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

        agg_demo_keys.report_user_segment_geo_region_area,
        ----------------------------------------

        COALESCE(metrics.cfy_deal_count,0)                           AS cfy_deal_count,
        COALESCE(metrics.cfy_booked_deal_count,0)                    AS cfy_booked_deal_count,
        COALESCE(metrics.cfy_churned_contraction_deal_count,0)       AS cfy_churned_contraction_deal_count,

        COALESCE(metrics.cfy_open_1plus_deal_count,0)                AS cfy_open_1plus_deal_count,
        COALESCE(metrics.cfy_open_3plus_deal_count,0)                AS cfy_open_3plus_deal_count,
        COALESCE(metrics.cfy_open_4plus_deal_count,0)                AS cfy_open_4plus_deal_count,

        -----------------------------------------------------------------------------------
        -- NF: 20210201  NET ARR fields
        
        COALESCE(metrics.cfy_open_1plus_net_arr,0)                   AS cfy_open_1plus_net_arr,
        COALESCE(metrics.cfy_open_3plus_net_arr,0)                   AS cfy_open_3plus_net_arr,
        COALESCE(metrics.cfy_open_4plus_net_arr,0)                   AS cfy_open_4plus_net_arr,
        COALESCE(metrics.cfy_booked_net_arr,0)                       AS cfy_booked_net_arr,

      -- pipe gen
        COALESCE(metrics.cfy_pipe_gen_count,0)                       AS cfy_pipe_gen_count,
        COALESCE(metrics.cfy_pipe_gen_net_arr,0)                     AS cfy_pipe_gen_net_arr,

        -- sao gen
        COALESCE(metrics.cfy_sao_deal_count,0)                        AS cfy_sao_deal_count,
        COALESCE(metrics.cfy_sao_net_arr,0)                          AS cfy_sao_net_arr,

        -- churned net_arr
        COALESCE(metrics.cfy_churned_contraction_net_arr,0)          AS cfy_churned_contraction_net_arr,

        -----------------------------------------------------------------------------------

        -- targets current quarter
        COALESCE(targets.target_net_arr,0)                        AS target_net_arr,
        COALESCE(targets.target_deal_count,0)                     AS target_deal_count,
        COALESCE(targets.target_pipe_generation_net_arr,0)        AS target_pipe_generation_net_arr, 
  
        COALESCE(total_fy_deal_count,0)                       AS total_fy_deal_count,
        COALESCE(total_fy_booked_deal_count,0)                AS total_fy_booked_deal_count,
        COALESCE(total_fy_churned_contraction_deal_count,0)   AS total_fy_churned_contraction_deal_count,

        COALESCE(total_fy_booked_net_arr,0)                   AS total_fy_booked_net_arr,
        COALESCE(total_fy_churned_contraction_net_arr,0)      AS total_fy_churned_contraction_net_arr,

        -- pipe gen
        COALESCE(total_fy_pipe_gen_count,0)                   AS total_fy_pipe_gen_count,
        COALESCE(total_fy_pipe_gen_net_arr,0)                 AS total_fy_pipe_gen_net_arr,

        -- sao gen
        COALESCE(total_fy_sao_deal_count,0)                   AS total_fy_sao_deal_count,
        COALESCE(total_fy_sao_net_arr,0)                      AS total_fy_sao_net_arr,
  
        COALESCE(targets.calculated_target_net_arr,0)             AS calculated_target_net_arr, 
        COALESCE(targets.calculated_target_deal_count,0)          AS calculated_target_deal_count,  
        COALESCE(targets.calculated_target_pipe_generation,0)     AS calculated_target_pipe_generation,

      -- TIMESTAMP
      current_timestamp                                              AS dbt_last_run_at
  
    FROM base_fields base 
    -- base keys dictionary
    LEFT JOIN agg_demo_keys
      ON base.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN consolidated_metrics metrics
      ON metrics.close_fiscal_year = base.close_fiscal_year
      AND metrics.close_day_of_fiscal_year_normalised = base.close_day_of_fiscal_year_normalised
      AND metrics.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- current quarter
    LEFT JOIN consolidated_targets_per_day targets 
      ON targets.close_fiscal_year = base.close_fiscal_year
        AND targets.close_day_of_fiscal_year_normalised = base.close_day_of_fiscal_year_normalised
        AND targets.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot

)
 SELECT *
 FROM final
 