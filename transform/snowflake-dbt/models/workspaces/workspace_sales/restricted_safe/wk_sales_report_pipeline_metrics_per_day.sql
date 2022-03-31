{{ config(alias='report_pipeline_metrics_day') }}

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}  


), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 

), sfdc_opportunity_xf AS (

    SELECT 
        opties.*,
        today_date.date_actual                      AS snapshot_date,
        today_date.day_of_fiscal_quarter_normalised AS snapshot_day_of_fiscal_quarter_normalised,
        today_date.fiscal_quarter_name_fy           AS snapshot_fiscal_quarter_name,
        today_date.first_day_of_fiscal_quarter      AS snapshot_fiscal_quarter_date,
        today_date.fiscal_year                      AS snapshot_fiscal_year,

        -- created within quarter net arr
        CASE
          WHEN opties.pipeline_created_fiscal_quarter_name = today_date.fiscal_quarter_name_fy  
            AND opties.is_eligible_created_pipeline_flag = 1
              THEN opties.net_arr
          ELSE 0 
        END                                                AS created_in_snapshot_quarter_net_arr,
     
      -- created within quarter deal count
        CASE
          WHEN opties.pipeline_created_fiscal_quarter_name = today_date.fiscal_quarter_name_fy  
            AND opties.is_eligible_created_pipeline_flag = 1
              THEN opties.calculated_deal_count
          ELSE 0 
        END                                                AS created_in_snapshot_quarter_deal_count
          
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}} opties
    CROSS JOIN (SELECT *
                  FROM date_details
                  WHERE date_actual = CURRENT_DATE) today_date 
    WHERE opties.is_deleted = 0
      AND opties.is_excluded_flag = 0
      AND opties.is_edu_oss = 0
      AND opties.net_arr is not null
      AND lower(opties.deal_group) LIKE ANY ('%growth%', '%new%')
      AND opties.stage_name NOT IN ('0-Pending Acceptance','Unqualified','00-Pre Opportunity','9-Unqualified','10-Duplicate')  
      -- Not JiHu account
  
), sfdc_opportunity_snapshot_history_xf AS (

    SELECT opp_snapshot.*
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}} opp_snapshot
    WHERE opp_snapshot.is_deleted = 0
      AND opp_snapshot.is_excluded_flag = 0
      AND opp_snapshot.is_edu_oss = 0
      AND opp_snapshot.net_arr is not null
      AND lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
      -- include up to current date, where we use the current opportunity object
      AND opp_snapshot.snapshot_date < CURRENT_DATE
      -- stage 1 plus, won & lost excluded ommited deals    
      AND opp_snapshot.stage_name NOT IN ('0-Pending Acceptance','Unqualified','00-Pre Opportunity','9-Unqualified','10-Duplicate') 
       -- Not JiHu account
 
), pipeline_snapshot AS (

    SELECT 
      -------------------------------------
      -- report keys
      opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
       
      -------------------------------------
      
      -----------------------------------------------------------------------------------
      -- snapshot date fields
      opp_snapshot.snapshot_date,
      opp_snapshot.snapshot_fiscal_year,
      opp_snapshot.snapshot_fiscal_quarter_name,
      opp_snapshot.snapshot_fiscal_quarter_date,
      opp_snapshot.snapshot_day_of_fiscal_quarter_normalised,
      -----------------------------------------------------------------------------------
    
      opp_snapshot.stage_name,
      opp_snapshot.forecast_category_name,
      opp_snapshot.is_renewal,
      opp_snapshot.is_won,
      opp_snapshot.is_lost,
      opp_snapshot.is_open,
      opp_snapshot.is_excluded_flag,
      
      opp_snapshot.close_fiscal_quarter_name,
      opp_snapshot.close_fiscal_quarter_date,
      opp_snapshot.created_fiscal_quarter_name,
      opp_snapshot.created_fiscal_quarter_date,

      opp_snapshot.net_arr,

      opp_snapshot.calculated_deal_count                AS deal_count,

      opp_snapshot.open_1plus_deal_count,
      opp_snapshot.open_3plus_deal_count,
      opp_snapshot.open_4plus_deal_count,
  
      -- booked deal count
      opp_snapshot.booked_deal_count,
      opp_snapshot.churned_contraction_deal_count,
  
      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      opp_snapshot.open_1plus_net_arr,
      opp_snapshot.open_3plus_net_arr,
      opp_snapshot.open_4plus_net_arr,
  
      -- booked net _arr
      opp_snapshot.booked_net_arr,

      -- churned net_arr
      opp_snapshot.churned_contraction_net_arr,
  
      opp_snapshot.created_and_won_same_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_deal_count

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
    -- Keep the UNION ALL, somehow UNION is losing data
    UNION ALL
    SELECT 
      -------------------------------------
      -- report keys
      opties.report_user_segment_geo_region_area_sqs_ot,
      -------------------------------------
      
      -----------------------------------------------------------------------------------
      -- snapshot date fields
      opties.snapshot_date,
      opties.snapshot_fiscal_year,
      opties.snapshot_fiscal_quarter_name,
      opties.snapshot_fiscal_quarter_date,
      opties.snapshot_day_of_fiscal_quarter_normalised,
      -----------------------------------------------------------------------------------
    
      opties.stage_name,
      opties.forecast_category_name,
      opties.is_renewal,
      opties.is_won,
      opties.is_lost,
      opties.is_open,
      
      opties.is_excluded_flag,
      
      opties.close_fiscal_quarter_name,
      opties.close_fiscal_quarter_date,

      opties.created_fiscal_quarter_name,
      opties.created_fiscal_quarter_date,

      opties.net_arr,

      opties.calculated_deal_count                AS deal_count,

      opties.open_1plus_deal_count,
      opties.open_3plus_deal_count,
      opties.open_4plus_deal_count,
  
      -- booked deal count
      opties.booked_deal_count,
      opties.churned_contraction_deal_count,
  
      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      opties.open_1plus_net_arr,
      opties.open_3plus_net_arr,
      opties.open_4plus_net_arr,
  
      -- booked net _arr
      opties.booked_net_arr,

      -- churned net_arr
      opties.churned_contraction_net_arr,
  
      -- created and closed within the quarter net arr
      opties.created_and_won_same_quarter_net_arr,
      
      -- created within quarter
      opties.created_in_snapshot_quarter_net_arr,
     
      -- created within quarter
      opties.created_in_snapshot_quarter_deal_count

    FROM sfdc_opportunity_xf opties
  
), reported_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,
      
      -------------------
      -- report keys
      -- FY23 needs to be updated to the new logic
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(pipeline_snapshot.deal_count)                           AS deal_count,
      SUM(pipeline_snapshot.booked_deal_count)                    AS booked_deal_count,
      SUM(pipeline_snapshot.churned_contraction_deal_count)       AS churned_contraction_deal_count,

      SUM(pipeline_snapshot.open_1plus_deal_count)                AS open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                AS open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                AS open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                   AS open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                   AS open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                   AS open_4plus_net_arr,
      SUM(pipeline_snapshot.booked_net_arr)                       AS booked_net_arr,
      
      -- churned net_arr
      SUM(pipeline_snapshot.churned_contraction_net_arr)          AS churned_contraction_net_arr,
  
      SUM(pipeline_snapshot.created_and_won_same_quarter_net_arr) AS created_and_won_same_quarter_net_arr

      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- snapshot quarter rows that close within the same quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_name = pipeline_snapshot.close_fiscal_quarter_name
    GROUP BY 1,2,3
  
-- Quarter plus 1, from the reported quarter perspective
), report_quarter_plus_1 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,

      pipeline_snapshot.close_fiscal_quarter_name                   AS rq_plus_1_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date                   AS rq_plus_1_close_fiscal_quarter_date,
 
      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -------------------
     
      SUM(pipeline_snapshot.open_1plus_deal_count)         AS rq_plus_1_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)         AS rq_plus_1_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)         AS rq_plus_1_open_4plus_deal_count,

      ------------------------------
      -- Net ARR 

      SUM(pipeline_snapshot.open_1plus_net_arr)            AS rq_plus_1_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)            AS rq_plus_1_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)            AS rq_plus_1_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show rows in quarter plus 1 of snapshot quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -3,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND pipeline_snapshot.is_lost = 0  
    GROUP BY 1,2,3,4,5
  
-- Quarter plus 2, from the reported quarter perspective
), report_quarter_plus_2 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_date                AS close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised   AS close_day_of_fiscal_quarter_normalised,

      pipeline_snapshot.close_fiscal_quarter_name                   AS rq_plus_2_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date                   AS rq_plus_2_close_fiscal_quarter_date,

      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -------------------
     
      SUM(pipeline_snapshot.open_1plus_deal_count)           AS rq_plus_2_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)           AS rq_plus_2_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)           AS rq_plus_2_open_4plus_deal_count,
      
      -------------------
      -- Net ARR 
      -- Use Net ARR instead

      SUM(pipeline_snapshot.open_1plus_net_arr)              AS rq_plus_2_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)              AS rq_plus_2_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)              AS rq_plus_2_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show rows in quarter plus 2 of snapshot quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -6,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND pipeline_snapshot.is_lost = 0  
    GROUP BY 1,2,3,4,5
  
), pipeline_gen AS (

    SELECT
      opp_history.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opp_history.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opp_history.created_in_snapshot_quarter_deal_count)     AS pipe_gen_count,

      -- Net ARR 
      SUM(opp_history.created_in_snapshot_quarter_net_arr)        AS pipe_gen_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.pipeline_created_fiscal_quarter_name
      AND opp_history.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2,3
    -- Keep the UNION ALL, somehow UNION is losing data
    UNION ALL
    SELECT
      opties.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opties.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opties.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opties.created_in_snapshot_quarter_deal_count)     AS pipe_gen_count,

      -- Net ARR 
      SUM(opties.created_in_snapshot_quarter_net_arr)        AS pipe_gen_net_arr

    FROM sfdc_opportunity_xf opties
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opties.snapshot_fiscal_quarter_name = opties.pipeline_created_fiscal_quarter_name
      AND opties.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2,3

--Sales Accepted Opportunities
), sao_gen AS (

    SELECT
      opp_history.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opp_history.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opp_history.calculated_deal_count)     AS sao_deal_count,

      -- Net ARR 
      SUM(opp_history.net_arr)                  AS sao_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.sales_accepted_fiscal_quarter_name
      AND opp_history.is_eligible_sao_flag = 1
    GROUP BY 1,2,3
    -- Keep the UNION ALL, somehow UNION is losing data
    UNION ALL
    SELECT
      opties.snapshot_fiscal_quarter_date              AS close_fiscal_quarter_date,
      opties.snapshot_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opties.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opties.calculated_deal_count)     AS sao_deal_count,

      -- Net ARR 
      SUM(opties.net_arr)                   AS sao_net_arr

    FROM sfdc_opportunity_xf opties
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opties.snapshot_fiscal_quarter_name = opties.sales_accepted_fiscal_quarter_name
      AND opties.is_eligible_sao_flag = 1
    GROUP BY 1,2,3

-- These CTE builds a complete set of values 
), key_fields AS (
  
 SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_quarter_date
  FROM reported_quarter
  UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_quarter_date
  FROM report_quarter_plus_1
  UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_quarter_date
  FROM report_quarter_plus_2
  UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_quarter_date
  FROM pipeline_gen
  UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_quarter_date
  FROM sao_gen

), base_fields AS (
  
  SELECT 
      key_fields.*,
      close_date.fiscal_quarter_name_fy               AS close_fiscal_quarter_name,
      close_date.date_actual                          AS close_date,
      close_date.day_of_fiscal_quarter_normalised     AS close_day_of_fiscal_quarter_normalised,
      close_date.fiscal_year                          AS close_fiscal_year,
      rq_plus_1.first_day_of_fiscal_quarter           AS rq_plus_1_close_fiscal_quarter_date,
      rq_plus_1.fiscal_quarter_name_fy                AS rq_plus_1_close_fiscal_quarter_name,
      rq_plus_2.first_day_of_fiscal_quarter           AS rq_plus_2_close_fiscal_quarter_date,
      rq_plus_2.fiscal_quarter_name_fy                AS rq_plus_2_close_fiscal_quarter_name
  FROM key_fields
  INNER JOIN date_details close_date
    ON close_date.first_day_of_fiscal_quarter = key_fields.close_fiscal_quarter_date
  LEFT JOIN date_details rq_plus_1
    ON rq_plus_1.date_actual = dateadd(month,3,close_date.first_day_of_fiscal_quarter) 
  LEFT JOIN date_details rq_plus_2
    ON rq_plus_2.date_actual = dateadd(month,6,close_date.first_day_of_fiscal_quarter)  

), report_pipeline_metrics_day AS (
      
    SELECT 
      -----------------------------
      -- keys
      base_fields.report_user_segment_geo_region_area_sqs_ot,

      base_fields.close_fiscal_quarter_date,
      base_fields.close_fiscal_quarter_name,
      base_fields.close_fiscal_year,
      base_fields.close_date,
      base_fields.close_day_of_fiscal_quarter_normalised,
      -----------------------------

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

      agg_demo_keys.report_user_segment_geo_region_area,

      -- used to track the latest updated day in the model
      -- this might be different to the latest available information in the source models
      -- as dbt runs are not necesarly in synch
      CASE 
        WHEN base_fields.close_date = CURRENT_DATE
          THEN 1
          ELSE 0
      END                                                         AS is_today_flag,

      -- report quarter plus 1 / 2 date fields
      base_fields.rq_plus_1_close_fiscal_quarter_name,
      base_fields.rq_plus_1_close_fiscal_quarter_date,
      base_fields.rq_plus_2_close_fiscal_quarter_name,
      base_fields.rq_plus_2_close_fiscal_quarter_date,    
  
      -- reported quarter
      COALESCE(reported_quarter.deal_count,0)                     AS deal_count,
      COALESCE(reported_quarter.open_1plus_deal_count,0)          AS open_1plus_deal_count,
      COALESCE(reported_quarter.open_3plus_deal_count,0)          AS open_3plus_deal_count,
      COALESCE(reported_quarter.open_4plus_deal_count,0)          AS open_4plus_deal_count, 
      COALESCE(reported_quarter.booked_deal_count,0)              AS booked_deal_count,
      -- churned deal count
      COALESCE(reported_quarter.churned_contraction_deal_count,0)         AS churned_contraction_deal_count,
     


      -- reported quarter + 1
      COALESCE(report_quarter_plus_1.rq_plus_1_open_1plus_deal_count,0)    AS rq_plus_1_open_1plus_deal_count,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_3plus_deal_count,0)    AS rq_plus_1_open_3plus_deal_count,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_4plus_deal_count,0)    AS rq_plus_1_open_4plus_deal_count,
  
      -- reported quarter + 2
      COALESCE(report_quarter_plus_2.rq_plus_2_open_1plus_deal_count,0)    AS rq_plus_2_open_1plus_deal_count,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_3plus_deal_count,0)    AS rq_plus_2_open_3plus_deal_count,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_4plus_deal_count,0)    AS rq_plus_2_open_4plus_deal_count,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead     
      -- created and closed

      -- reported quarter
      COALESCE(reported_quarter.booked_net_arr,0)                 AS booked_net_arr,
      -- churned net_arr
      COALESCE(reported_quarter.churned_contraction_net_arr,0)    AS churned_contraction_net_arr,
      COALESCE(reported_quarter.open_1plus_net_arr,0)             AS open_1plus_net_arr,
      COALESCE(reported_quarter.open_3plus_net_arr,0)             AS open_3plus_net_arr, 
      COALESCE(reported_quarter.open_4plus_net_arr,0)             AS open_4plus_net_arr, 

      COALESCE(reported_quarter.created_and_won_same_quarter_net_arr,0)     AS created_and_won_same_quarter_net_arr,


        -- reported quarter + 1
      COALESCE(report_quarter_plus_1.rq_plus_1_open_1plus_net_arr,0)       AS rq_plus_1_open_1plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_3plus_net_arr,0)       AS rq_plus_1_open_3plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_4plus_net_arr,0)       AS rq_plus_1_open_4plus_net_arr,

      -- reported quarter + 2
      COALESCE(report_quarter_plus_2.rq_plus_2_open_1plus_net_arr,0)       AS rq_plus_2_open_1plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_3plus_net_arr,0)       AS rq_plus_2_open_3plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_4plus_net_arr,0)       AS rq_plus_2_open_4plus_net_arr,

      -- pipe gen
      COALESCE(pipeline_gen.pipe_gen_count,0)                             AS pipe_gen_count,
      COALESCE(pipeline_gen.pipe_gen_net_arr,0)                           AS pipe_gen_net_arr,

       -- sao gen
      COALESCE(sao_gen.sao_deal_count,0)                                  AS sao_deal_count,
      COALESCE(sao_gen.sao_net_arr,0)                                     AS sao_net_arr,

       -- one year ago sao gen
      COALESCE(minus_1_year_sao_gen.sao_net_arr,0)                      AS minus_1_year_sao_net_arr,
      COALESCE(minus_1_year_sao_gen.sao_deal_count,0)                   AS minus_1_year_sao_deal_count,

      -- one year ago pipe gen
      COALESCE(minus_1_year_pipe_gen.pipe_gen_net_arr,0)                AS minus_1_year_pipe_gen_net_arr,
      COALESCE(minus_1_year_pipe_gen.pipe_gen_count,0)                  AS minus_1_year_pipe_gen_deal_count,

      -- TIMESTAMP
      current_timestamp                                                 AS dbt_last_run_at

    -- created a list of all options to avoid having blanks when attaching metrics
    FROM base_fields
    -- base keys dictionary
    LEFT JOIN agg_demo_keys
      ON base_fields.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    -- historical quarter
    LEFT JOIN reported_quarter
      ON base_fields.close_day_of_fiscal_quarter_normalised = reported_quarter.close_day_of_fiscal_quarter_normalised
      AND base_fields.close_fiscal_quarter_date = reported_quarter.close_fiscal_quarter_date   
      AND base_fields.report_user_segment_geo_region_area_sqs_ot = reported_quarter.report_user_segment_geo_region_area_sqs_ot
    -- next quarter in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_1
      ON base_fields.close_day_of_fiscal_quarter_normalised = report_quarter_plus_1.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = report_quarter_plus_1.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = report_quarter_plus_1.report_user_segment_geo_region_area_sqs_ot    
    -- 2 quarters ahead in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_2
      ON base_fields.close_day_of_fiscal_quarter_normalised = report_quarter_plus_2.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = report_quarter_plus_2.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = report_quarter_plus_2.report_user_segment_geo_region_area_sqs_ot
    -- Pipe generation piece
    LEFT JOIN pipeline_gen 
      ON base_fields.close_day_of_fiscal_quarter_normalised = pipeline_gen.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = pipeline_gen.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = pipeline_gen.report_user_segment_geo_region_area_sqs_ot
    -- Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen
       ON base_fields.close_day_of_fiscal_quarter_normalised = sao_gen.close_day_of_fiscal_quarter_normalised
        AND base_fields.close_fiscal_quarter_date = sao_gen.close_fiscal_quarter_date   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = sao_gen.report_user_segment_geo_region_area_sqs_ot
    -- One Year Ago  pipeline generation
    LEFT JOIN pipeline_gen  minus_1_year_pipe_gen
      ON minus_1_year_pipe_gen.close_day_of_fiscal_quarter_normalised = base_fields.close_day_of_fiscal_quarter_normalised
        AND minus_1_year_pipe_gen.close_fiscal_quarter_date = DATEADD(month, -12, base_fields.close_fiscal_quarter_date)
        AND minus_1_year_pipe_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
    -- One Year Ago Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen minus_1_year_sao_gen
      ON minus_1_year_sao_gen.close_day_of_fiscal_quarter_normalised = base_fields.close_day_of_fiscal_quarter_normalised
        AND minus_1_year_sao_gen.close_fiscal_quarter_date = DATEADD(month, -12, base_fields.close_fiscal_quarter_date)
        AND minus_1_year_sao_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot

)

SELECT *
FROM report_pipeline_metrics_day
