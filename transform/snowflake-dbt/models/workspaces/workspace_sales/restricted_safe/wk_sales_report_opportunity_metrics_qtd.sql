{{ config(alias='report_opportunity_metrics_qtd') }}

WITH sfdc_opportunity_xf AS (
  
    SELECT * 
    FROM {{ ref('wk_sales_sfdc_opportunity_xf') }} 
    WHERE is_edu_oss = 0
    AND is_deleted = 0
  
), sfdc_opportunity_snapshot_history_xf AS (
   
   
    SELECT * 
    FROM {{ ref('wk_sales_sfdc_opportunity_snapshot_history_xf') }} 
    WHERE is_edu_oss = 0
    AND is_deleted = 0

), date_details AS (
  
    SELECT *
    FROM {{ ref('wk_sales_date_details') }} 

 ), agg_demo_keys AS (
    -- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 
   
), today AS (

    SELECT DISTINCT 
        date_actual                             AS current_date_actual,
        fiscal_quarter_name_fy                  AS current_fiscal_quarter_name,
        first_day_of_fiscal_quarter             AS current_fiscal_quarter_date,
        day_of_fiscal_quarter_normalised        AS current_fiscal_quarter_day_normalised,
        fiscal_year                             AS current_fiscal_year
    FROM date_details
    WHERE date_actual = CURRENT_DATE
   
), aggregation AS (

    SELECT
      --------------------------------------------------------------------------------
      -------------------------------------------------------------------------------- 
      -- KEYS

      oppty.report_user_segment_geo_region_area_sqs_ot,
      today.current_fiscal_quarter_name,
      today.current_fiscal_quarter_day_normalised,
  
      --------------------------------------------------------------------------------
      --------------------------------------------------------------------------------
      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
                  AND oppty.is_stage_1_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                  AS qtd_open_1plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
                  AND oppty.is_stage_3_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                  AS qtd_open_3plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
                  AND oppty.is_stage_4_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count  
                ELSE 0 
            END)                                                                  AS qtd_open_4plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name 
              AND oppty.is_won = 1
                THEN oppty.calculated_deal_count  
              ELSE 0
          END)                                                                    AS qtd_closed_deal_count,

      -- CREATED in quarter
      -- NF: 2020-02-18 Need to validate how do I consider lost deals, a lost deal that is coming from a non- 1+stage should not oppty. included here
      SUM(CASE 
            WHEN oppty.pipeline_created_fiscal_quarter_name = today.current_fiscal_quarter_name 
              AND oppty.is_eligible_created_pipeline_flag = 1
                THEN oppty.calculated_deal_count 
              ELSE 0 
          END)                                                                    AS qtd_created_deal_count,

      -- NEXT Q Deal count
        -- Net ARR

      SUM(CASE
        WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
            THEN oppty.booked_net_arr
          ELSE 0 
      END)                                                                   AS qtd_booked_net_arr,
      
      SUM(CASE
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_1_plus = 1
                THEN oppty.net_arr 
              ELSE 0 
           END)                                                                   AS qtd_open_1plus_net_arr,

      SUM (CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_3_plus = 1
                THEN  oppty.net_arr
              ELSE 0 
            END)                                                                  AS qtd_open_3plus_net_arr,

      SUM (CASE 
            WHEN oppty.close_fiscal_quarter_name = today.current_fiscal_quarter_name
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_4_plus = 1
                THEN  oppty.net_arr 
            ELSE 0 
           END)                                                                  AS qtd_open_4plus_net_arr,

      -- created pipeline in quarter
      SUM(CASE
            WHEN oppty.pipeline_created_fiscal_quarter_name = today.current_fiscal_quarter_name 
            AND oppty.is_eligible_created_pipeline_flag = 1
                THEN oppty.net_arr 
              ELSE 0 
          END)                                                                    AS qtd_created_net_arr,

      -- created and closed in the same quarter
      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_name = current_fiscal_quarter_name 
              AND oppty.pipeline_created_fiscal_quarter_name = today.current_fiscal_quarter_name
              AND oppty.is_eligible_created_pipeline_flag = 1
              AND  (oppty.is_won = 1 OR (oppty.is_lost = 1 AND oppty.is_renewal = 1))
                THEN oppty.net_arr
              ELSE 0 
          END)                                                                   AS qtd_created_and_closed_net_arr,

      -----------------------------------------------------------------------------------------------------------
      -----------------------------------------------------------------------------------------------------------

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
                  AND oppty.is_stage_1_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                 AS rq_plus_1_open_1plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
                  AND oppty.is_stage_3_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                 AS rq_plus_1_open_3plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
                  AND oppty.is_stage_4_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1
              THEN oppty.calculated_deal_count  
                ELSE 0 
            END)                                                                 AS rq_plus_1_open_4plus_deal_count,
   
      -- next quarter net arr
      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_1_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_1_open_1plus_net_arr,

      -- next quarter 3+
      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_3_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_1_open_3plus_net_arr,

      -- next quarter 4+
      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,3,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_4_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_1_open_4plus_net_arr,
   
      -----------------------------------------------------------------------------------------------------------
      -----------------------------------------------------------------------------------------------------------
      -- quarter + 2
      -- DEAL COUNT
      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_1_plus = 1
              THEN oppty.calculated_deal_count 
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_1plus_deal_count,

      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_3_plus = 1
              THEN oppty.calculated_deal_count 
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_3plus_deal_count,

      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_4_plus = 1
              THEN oppty.calculated_deal_count
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_4plus_deal_count,
      -------------
      -- NET ARR
      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_1_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_1plus_net_arr,

      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_3_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_3plus_net_arr,

      SUM(CASE  
          WHEN oppty.close_fiscal_quarter_date = DATEADD(month,6,today.current_fiscal_quarter_date)
            AND oppty.is_eligible_open_pipeline_flag = 1
            AND oppty.is_stage_4_plus = 1
              THEN oppty.net_arr 
            ELSE 0 
          END)                                                                  AS rq_plus_2_open_4plus_net_arr,

    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    --------------------------------------------------------------------------------
    -- Fiscal year metrics
  
      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
                  AND oppty.is_stage_1_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                  AS cfy_open_1plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
                  AND oppty.is_stage_3_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count 
                ELSE 0 
            END)                                                                  AS cfy_open_3plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
                  AND oppty.is_stage_4_plus = 1 
                  AND oppty.is_eligible_open_pipeline_flag = 1 
              THEN oppty.calculated_deal_count  
                ELSE 0 
            END)                                                                  AS cfy_open_4plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.is_won = 1
                THEN oppty.calculated_deal_count  
              ELSE 0
          END)                                                                    AS cfy_closed_deal_count,

      -- CREATED in quarter
      -- NF: 2020-02-18 Need to validate how do I consider lost deals, a lost deal that is coming from a non- 1+stage should not oppty. included here
      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.is_eligible_created_pipeline_flag = 1
                THEN oppty.calculated_deal_count 
              ELSE 0 
          END)                                                                    AS cfy_created_deal_count,

      -- NEXT Q Deal count
      -- Net ARR

      SUM(CASE
        WHEN oppty.close_fiscal_year = today.current_fiscal_year
            THEN oppty.booked_net_arr
          ELSE 0 
      END)                                                                        AS cfy_booked_net_arr,
      
      SUM(CASE
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_1_plus = 1
                THEN oppty.net_arr 
              ELSE 0 
           END)                                                                   AS cfy_open_1plus_net_arr,

      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_3_plus = 1
                THEN  oppty.net_arr
              ELSE 0 
            END)                                                                  AS cfy_open_3plus_net_arr,

      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_4_plus = 1
                THEN  oppty.net_arr 
            ELSE 0 
           END)                                                                   AS cfy_open_4plus_net_arr,

      -- created pipeline in year
      SUM(CASE
            WHEN oppty.pipeline_created_fiscal_year = today.current_fiscal_year
            AND oppty.is_eligible_created_pipeline_flag = 1
                THEN oppty.net_arr 
              ELSE 0 
          END)                                                                    AS cfy_created_net_arr,

      -- created and closed in the same year
      SUM(CASE 
            WHEN oppty.close_fiscal_year = today.current_fiscal_year
              AND oppty.pipeline_created_fiscal_year = today.current_fiscal_year
              AND oppty.is_eligible_created_pipeline_flag = 1
              AND  (oppty.is_won = 1 OR (oppty.is_lost = 1 AND oppty.is_renewal = 1))
                THEN oppty.net_arr
              ELSE 0 
          END)                                                                   AS cfy_created_and_closed_net_arr,

      -----------------------------------------------------------------------------------------------------------
      -----------------------------------------------------------------------------------------------------------
      -- next 4 quarters

      -- DEAL COUNT      
      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_stage_1_plus = 1 
              AND oppty.is_eligible_open_pipeline_flag = 1
                THEN oppty.calculated_deal_count 
            ELSE 0 
          END)                                                                   AS next_4q_open_1plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_stage_3_plus = 1 
              AND oppty.is_eligible_open_pipeline_flag = 1
                THEN oppty.calculated_deal_count 
            ELSE 0 
          END)                                                                  AS next_4q_open_3plus_deal_count,

      SUM(CASE 
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_stage_4_plus = 1 
              AND oppty.is_eligible_open_pipeline_flag = 1
                THEN oppty.calculated_deal_count  
            ELSE 0 
          END)                                                                 AS next_4q_open_4plus_deal_count,
      
      -- NET ARR
      SUM(CASE
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
                THEN oppty.booked_net_arr
            ELSE 0 
          END)                                                                 AS next_4q_booked_net_arr,            
  
      SUM(CASE  
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_1_plus = 1
                THEN oppty.net_arr 
            ELSE 0 
          END)                                                                 AS next_4q_open_1plus_net_arr,

      SUM(CASE  
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_3_plus = 1
                THEN oppty.net_arr 
            ELSE 0 
          END)                                                                 AS next_4q_open_3plus_net_arr,

      SUM(CASE  
            WHEN oppty.close_fiscal_quarter_date >= today.current_fiscal_quarter_date
              AND oppty.close_fiscal_quarter_date <= DATEADD(month,12,today.current_fiscal_quarter_date)
              AND oppty.is_eligible_open_pipeline_flag = 1
              AND oppty.is_stage_4_plus = 1
                THEN oppty.net_arr 
            ELSE 0 
          END)                                                                 AS next_4q_open_4plus_net_arr

    FROM sfdc_opportunity_xf oppty
    -- identify todays quarter and fiscal quarter
    CROSS JOIN today
    GROUP BY 1,2,3

), pipe_gen_yoy AS (
  
    SELECT 
      opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
      SUM(opp_snapshot.net_arr)                            AS minus_1_year_pipe_gen_net_arr
    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
      CROSS JOIN today
    WHERE opp_snapshot.snapshot_fiscal_quarter_date = opp_snapshot.pipeline_created_fiscal_quarter_date
      AND opp_snapshot.is_eligible_created_pipeline_flag = 1
      AND opp_snapshot.snapshot_fiscal_quarter_date = DATEADD(month,-12,today.current_fiscal_quarter_date)
      AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = today.current_fiscal_quarter_day_normalised
      AND opp_snapshot.is_edu_oss = 0
      AND opp_snapshot.is_deleted = 0
      AND opp_snapshot.is_excluded_flag = 0
      AND lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
    GROUP BY 1
  
), report_opportunity_metrics_qtd AS (  

    SELECT 
        agg.*, 
        pipe_gen_yoy.minus_1_year_pipe_gen_net_arr,

        -- standard reporting keys
        COALESCE(agg_demo_keys.key_sqs,'other')                         AS key_sqs,
        COALESCE(agg_demo_keys.key_ot,'other')                          AS key_ot,

        COALESCE(agg_demo_keys.key_segment,'other')                     AS key_segment,
        COALESCE(agg_demo_keys.key_segment_sqs,'other')                 AS key_segment_sqs,                 
        COALESCE(agg_demo_keys.key_segment_ot,'other')                  AS key_segment_ot,       

        COALESCE(agg_demo_keys.key_segment_geo,'other')                 AS key_segment_geo,
        COALESCE(agg_demo_keys.key_segment_geo_sqs,'other')             AS key_segment_geo_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_ot,'other')              AS key_segment_geo_ot,      

        COALESCE(agg_demo_keys.key_segment_geo_region,'other')          AS key_segment_geo_region,
        COALESCE(agg_demo_keys.key_segment_geo_region_sqs,'other')      AS key_segment_geo_region_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_region_ot,'other')       AS key_segment_geo_region_ot,   

        COALESCE(agg_demo_keys.key_segment_geo_region_area,'other')     AS key_segment_geo_region_area,
        COALESCE(agg_demo_keys.key_segment_geo_region_area_sqs,'other') AS key_segment_geo_region_area_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_region_area_ot,'other')  AS key_segment_geo_region_area_ot,

        COALESCE(agg_demo_keys.report_opportunity_user_segment ,'other')  AS sales_team_cro_level,

        -- NF: This code replicates the reporting structured of FY22, to keep current tools working
        COALESCE(agg_demo_keys.sales_team_rd_asm_level,'other')           AS sales_team_rd_asm_level,
        COALESCE(agg_demo_keys.sales_team_vp_level,'other')               AS sales_team_vp_level,
        COALESCE(agg_demo_keys.sales_team_avp_rd_level,'other')           AS sales_team_avp_rd_level,
        COALESCE(agg_demo_keys.sales_team_asm_level,'other')              AS sales_team_asm_level

    FROM aggregation agg
    -- Add keys for aggregated analysis
    LEFT JOIN pipe_gen_yoy
        ON agg.report_user_segment_geo_region_area_sqs_ot = pipe_gen_yoy.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN agg_demo_keys
        ON agg.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
  )

SELECT *
FROM report_opportunity_metrics_qtd
