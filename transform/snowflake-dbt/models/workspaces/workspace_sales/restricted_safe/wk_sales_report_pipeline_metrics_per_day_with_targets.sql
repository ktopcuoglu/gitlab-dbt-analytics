{{ config(alias='report_pipeline_metrics_day_with_targets') }}

WITH date_details AS (
  
  SELECT *
  FROM   {{ ref('wk_sales_date_details') }} 
  
), report_pipeline_metrics_day AS (

   SELECT *
   FROM  {{ ref('wk_sales_report_pipeline_metrics_per_day') }} 

), report_targets_totals_per_quarter AS (

    SELECT *
    FROM  {{ ref('wk_sales_report_targets_totals_per_quarter') }} 

), mart_sales_funnel_target_daily AS (

  SELECT *
  FROM {{ref('wk_sales_mart_sales_funnel_target_daily')}}
  
), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 

-- make sure the aggregation works at the level we want it
), consolidated_metrics AS (

    SELECT 
        ---------------------------
        -- Keys
        report_user_segment_geo_region_area_sqs_ot,
        -----------------------------

        close_fiscal_quarter_date,
        close_fiscal_quarter_name,
        close_day_of_fiscal_quarter_normalised,

        -- reported quarter
        SUM(deal_count)                     AS deal_count,
        SUM(open_1plus_deal_count)          AS open_1plus_deal_count,
        SUM(open_3plus_deal_count)          AS open_3plus_deal_count,
        SUM(open_4plus_deal_count)          AS open_4plus_deal_count, 
        SUM(booked_deal_count)              AS booked_deal_count,
        SUM(churned_contraction_deal_count) AS churned_contraction_deal_count,



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
        SUM(churned_contraction_net_arr)        AS churned_contraction_net_arr,

        SUM(open_1plus_net_arr)                 AS open_1plus_net_arr,
        SUM(open_3plus_net_arr)                 AS open_3plus_net_arr, 
        SUM(open_4plus_net_arr)                 AS open_4plus_net_arr, 

        SUM(created_and_won_same_quarter_net_arr)       AS created_and_won_same_quarter_net_arr,

        -- reported quarter + 1
        SUM(rq_plus_1_open_1plus_net_arr)       AS rq_plus_1_open_1plus_net_arr,
        SUM(rq_plus_1_open_3plus_net_arr)       AS rq_plus_1_open_3plus_net_arr,
        SUM(rq_plus_1_open_4plus_net_arr)       AS rq_plus_1_open_4plus_net_arr,

        -- reported quarter + 2
        SUM(rq_plus_2_open_1plus_net_arr)       AS rq_plus_2_open_1plus_net_arr,
        SUM(rq_plus_2_open_3plus_net_arr)       AS rq_plus_2_open_3plus_net_arr,
        SUM(rq_plus_2_open_4plus_net_arr)       AS rq_plus_2_open_4plus_net_arr,

        -- pipe gen
        SUM(pipe_gen_count)                     AS pipe_gen_count,
        SUM(pipe_gen_net_arr)                   AS pipe_gen_net_arr,

        -- sao deal count
        SUM(sao_deal_count)                     AS sao_deal_count,
        SUM(sao_net_arr)                        AS sao_net_arr,

        -- one year ago pipe gen
        SUM(minus_1_year_pipe_gen_net_arr)      AS minus_1_year_pipe_gen_net_arr,
        SUM(minus_1_year_pipe_gen_deal_count)   AS minus_1_year_pipe_gen_deal_count,

        -- one year ago sao
        SUM(minus_1_year_sao_net_arr)           AS minus_1_year_sao_net_arr,
        SUM(minus_1_year_sao_deal_count)        AS minus_1_year_sao_deal_count


    FROM report_pipeline_metrics_day
    WHERE close_day_of_fiscal_quarter_normalised > 0
    GROUP BY 1,2,3,4
  
), consolidated_targets AS (

    SELECT
        ---------------------------
        -- Keys
        report_user_segment_geo_region_area_sqs_ot,
        -----------------------------

        close_fiscal_quarter_name,
        close_fiscal_quarter_date,
     
        close_fiscal_year,
     
        SUM(target_net_arr)                         AS target_net_arr,
        SUM(target_deal_count)                      AS target_deal_count,
        SUM(target_pipe_generation_net_arr)         AS target_pipe_generation_net_arr, 
  
        SUM(total_booked_net_arr)                           AS total_booked_net_arr,
        SUM(total_churned_contraction_net_arr)              AS total_churned_contraction_net_arr,
        SUM(total_booked_deal_count)                        AS total_booked_deal_count,
        SUM(total_churned_contraction_deal_count)           AS total_churned_contraction_deal_count,
        SUM(total_pipe_generation_net_arr)                  AS total_pipe_generation_net_arr,
        SUM(total_pipe_generation_deal_count)               AS total_pipe_generation_deal_count,
        SUM(total_created_and_booked_same_quarter_net_arr)  AS total_created_and_booked_same_quarter_net_arr,
        SUM(total_sao_generation_net_arr)                   AS total_sao_generation_net_arr,
        SUM(total_sao_generation_deal_count)                AS total_sao_generation_deal_count,
  
        SUM(calculated_target_net_arr)              AS calculated_target_net_arr, 
        SUM(calculated_target_deal_count)           AS calculated_target_deal_count,  
        SUM(calculated_target_pipe_generation)      AS calculated_target_pipe_generation
  FROM report_targets_totals_per_quarter
  GROUP BY 1,2,3,4

), consolidated_targets_per_day AS (
  
  SELECT 
        targets.*,
        close_day_of_fiscal_quarter_normalised
  FROM consolidated_targets targets
  CROSS JOIN (SELECT day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised
                FROM date_details
                WHERE day_of_fiscal_quarter_normalised > 0
                GROUP BY 1)


-- some of the funnel metrics have daily targets with a very specific seasonality
-- this models tracks the target allocated a given point in time on the quarter
), funnel_allocated_targets_qtd AS (

  SELECT 
    target_fiscal_quarter_date                AS close_fiscal_quarter_date,
    target_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,
    
    --------------------------
    report_user_segment_geo_region_area_sqs_ot,
       
    --------------------------

    SUM(CASE 
          WHEN kpi_name = 'Net ARR' 
            THEN qtd_allocated_target
           ELSE 0 
        END)                        AS qtd_target_net_arr,
    SUM(CASE 
          WHEN kpi_name = 'Deals' 
            THEN qtd_allocated_target
           ELSE 0 
        END)                        AS qtd_target_deal_count,
    SUM(CASE 
          WHEN kpi_name = 'Net ARR Pipeline Created' 
            THEN qtd_allocated_target
           ELSE 0 
        END)                        AS qtd_target_pipe_generation_net_arr
  FROM mart_sales_funnel_target_daily
  GROUP BY 1,2,3

), key_fields AS (
    
  SELECT         
      report_user_segment_geo_region_area_sqs_ot,
      close_fiscal_quarter_date
  FROM consolidated_targets
  UNION
  SELECT         
      report_user_segment_geo_region_area_sqs_ot,
      close_fiscal_quarter_date
  FROM consolidated_metrics
  
), base_fields AS (
  
 SELECT 
    key_fields.*,
    close_date.fiscal_quarter_name_fy              AS close_fiscal_quarter_name,
    close_date.fiscal_year                         AS close_fiscal_year,
    close_date.day_of_fiscal_quarter_normalised    AS close_day_of_fiscal_quarter_normalised,
    close_date.date_actual                         AS close_date,
    rq_plus_1.first_day_of_fiscal_quarter          AS rq_plus_1_close_fiscal_quarter_date,
    rq_plus_1.fiscal_quarter_name_fy               AS rq_plus_1_close_fiscal_quarter_name,
    rq_plus_2.first_day_of_fiscal_quarter          AS rq_plus_2_close_fiscal_quarter_date,
    rq_plus_2.fiscal_quarter_name_fy               AS rq_plus_2_close_fiscal_quarter_name
 FROM key_fields
 INNER JOIN date_details close_date
    ON close_date.first_day_of_fiscal_quarter = key_fields.close_fiscal_quarter_date
 LEFT JOIN date_details rq_plus_1
    ON rq_plus_1.date_actual = dateadd(month,3,close_date.first_day_of_fiscal_quarter) 
 LEFT JOIN date_details rq_plus_2
    ON rq_plus_2.date_actual = dateadd(month,6,close_date.first_day_of_fiscal_quarter)  
  
), final AS (
  
    SELECT 

        --------------------------
        -- keys
        base.report_user_segment_geo_region_area_sqs_ot,
        --------------------------
  
        base.close_fiscal_quarter_date,
        base.close_fiscal_quarter_name,
        base.close_fiscal_year,
        base.close_date,
        base.close_day_of_fiscal_quarter_normalised,

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

        -- report quarter plus 1 / 2 date fields
        base.rq_plus_1_close_fiscal_quarter_name,
        base.rq_plus_1_close_fiscal_quarter_date,
        base.rq_plus_2_close_fiscal_quarter_name,
        base.rq_plus_2_close_fiscal_quarter_date,    

        -- reported quarter
        metrics.deal_count,
        metrics.open_1plus_deal_count,
        metrics.open_3plus_deal_count,
        metrics.open_4plus_deal_count, 
        metrics.booked_deal_count,
        metrics.churned_contraction_deal_count,

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
        metrics.churned_contraction_net_arr,
        metrics.open_1plus_net_arr,
        metrics.open_3plus_net_arr, 
        metrics.open_4plus_net_arr, 

        -- pipe gen
        metrics.created_and_won_same_quarter_net_arr,
        metrics.pipe_gen_count,
        metrics.pipe_gen_net_arr,

        -- sao gen
        metrics.sao_deal_count,
        metrics.sao_net_arr,

        -- one year ago pipe gen
        metrics.minus_1_year_pipe_gen_net_arr,
        metrics.minus_1_year_pipe_gen_deal_count,

        -- one year ago sao
        metrics.minus_1_year_sao_net_arr,
        metrics.minus_1_year_sao_deal_count,

        -- reported quarter + 1
        metrics.rq_plus_1_open_1plus_net_arr,
        metrics.rq_plus_1_open_3plus_net_arr,
        metrics.rq_plus_1_open_4plus_net_arr,

        -- reported quarter + 2
        metrics.rq_plus_2_open_1plus_net_arr,
        metrics.rq_plus_2_open_3plus_net_arr,
        metrics.rq_plus_2_open_4plus_net_arr,

        -- targets current quarter
        COALESCE(targets.target_net_arr,0)                        AS target_net_arr,
        COALESCE(targets.target_deal_count,0)                     AS target_deal_count,
        COALESCE(targets.target_pipe_generation_net_arr,0)        AS target_pipe_generation_net_arr, 
  
        COALESCE(targets.total_booked_net_arr,0)                            AS total_booked_net_arr,
        COALESCE(targets.total_churned_contraction_net_arr,0)               AS total_churned_contraction_net_arr,
        COALESCE(targets.total_booked_deal_count,0)                         AS total_booked_deal_count,
        COALESCE(targets.total_churned_contraction_deal_count,0)            AS total_churned_contraction_deal_count,        
        COALESCE(targets.total_pipe_generation_net_arr,0)                   AS total_pipe_generation_net_arr,
        COALESCE(targets.total_pipe_generation_deal_count,0)                AS total_pipe_generation_deal_count,
        COALESCE(targets.total_created_and_booked_same_quarter_net_arr,0)   AS total_created_and_booked_same_quarter_net_arr,
        COALESCE(targets.total_sao_generation_net_arr,0)                    AS total_sao_generation_net_arr,
        COALESCE(targets.total_sao_generation_deal_count,0)                 AS total_sao_generation_deal_count,
  
        COALESCE(targets.calculated_target_net_arr,0)             AS calculated_target_net_arr, 
        COALESCE(targets.calculated_target_deal_count,0)          AS calculated_target_deal_count,  
        COALESCE(targets.calculated_target_pipe_generation,0)     AS calculated_target_pipe_generation,
  
        -- totals quarter plus 1
        COALESCE(rq_plus_one.total_booked_net_arr,0)            AS rq_plus_1_total_booked_net_arr,
        COALESCE(rq_plus_one.total_booked_deal_count,0)         AS rq_plus_1_total_booked_deal_count,
        COALESCE(rq_plus_one.target_net_arr,0)                  AS rq_plus_1_target_net_arr,
        COALESCE(rq_plus_one.target_deal_count,0)               AS rq_plus_1_target_deal_count,
        COALESCE(rq_plus_one.calculated_target_net_arr,0)       AS rq_plus_1_calculated_target_net_arr,
        COALESCE(rq_plus_one.calculated_target_deal_count,0)    AS rq_plus_1_calculated_target_deal_count,
  
         -- totals quarter plus 2
        COALESCE(rq_plus_two.total_booked_net_arr,0)              AS rq_plus_2_total_booked_net_arr,
        COALESCE(rq_plus_two.total_booked_deal_count,0)           AS rq_plus_2_total_booked_deal_count,
        COALESCE(rq_plus_two.target_net_arr,0)                    AS rq_plus_2_target_net_arr,
        COALESCE(rq_plus_two.target_deal_count,0)                 AS rq_plus_2_target_deal_count,
        COALESCE(rq_plus_two.calculated_target_net_arr,0)         AS rq_plus_2_calculated_target_net_arr,
        COALESCE(rq_plus_two.calculated_target_deal_count,0)      AS rq_plus_2_calculated_target_deal_count,

        COALESCE(qtd_target.qtd_target_net_arr,0)                   AS qtd_target_net_arr,
        COALESCE(qtd_target.qtd_target_deal_count,0)                AS qtd_target_deal_count,
        COALESCE(qtd_target.qtd_target_pipe_generation_net_arr,0)   AS qtd_target_pipe_generation_net_arr,

        -- totals one year ago
        COALESCE(year_minus_one.total_booked_net_arr,0)             AS minus_1_year_total_booked_net_arr,
        COALESCE(year_minus_one.total_booked_deal_count,0)          AS minus_1_year_total_booked_deal_count,
        COALESCE(year_minus_one.total_pipe_generation_net_arr,0)    AS minus_1_year_total_pipe_generation_net_arr,
        COALESCE(year_minus_one.total_pipe_generation_deal_count,0) AS minus_1_year_total_pipe_generation_deal_count,

      -- TIMESTAMP
      current_timestamp                                              AS dbt_last_run_at
  
    FROM base_fields base 
    -- base keys dictionary
    LEFT JOIN agg_demo_keys
      ON base.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN consolidated_metrics metrics
      ON metrics.close_fiscal_quarter_date = base.close_fiscal_quarter_date
      AND metrics.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
      AND metrics.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- current quarter
    LEFT JOIN consolidated_targets_per_day targets 
      ON targets.close_fiscal_quarter_date = base.close_fiscal_quarter_date
        AND targets.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
        AND targets.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- quarter plus 1 targets
    LEFT JOIN consolidated_targets_per_day rq_plus_one
      ON rq_plus_one.close_fiscal_quarter_date = base.rq_plus_1_close_fiscal_quarter_date
        AND rq_plus_one.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
        AND rq_plus_one.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- quarter plus 2 targets
    LEFT JOIN consolidated_targets_per_day rq_plus_two
      ON rq_plus_two.close_fiscal_quarter_date = base.rq_plus_2_close_fiscal_quarter_date
        AND rq_plus_two.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
        AND rq_plus_two.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- qtd allocated targets
    LEFT JOIN funnel_allocated_targets_qtd qtd_target
      ON qtd_target.close_fiscal_quarter_date = base.close_fiscal_quarter_date
        AND qtd_target.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
        AND qtd_target.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot
    -- one year ago totals
    LEFT JOIN consolidated_targets_per_day year_minus_one
      ON year_minus_one.close_fiscal_quarter_date = dateadd(month,-12,base.close_fiscal_quarter_date)
        AND year_minus_one.close_day_of_fiscal_quarter_normalised = base.close_day_of_fiscal_quarter_normalised
        AND year_minus_one.report_user_segment_geo_region_area_sqs_ot = base.report_user_segment_geo_region_area_sqs_ot



)
 SELECT *
 FROM final
 