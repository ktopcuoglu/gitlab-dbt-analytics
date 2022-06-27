{{ config(alias='report_pipeline_fy_metrics_per_day') }}

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM workspace_sales.date_details


), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

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
    --FROM restricted_safe_workspace_sales.sfdc_opportunity_xf opties
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
    --FROM "NFIGUERA_PROD"."RESTRICTED_SAFE_WORKSPACE_SALES"."SFDC_OPPORTUNITY_SNAPSHOT_HISTORY_XF" opp_snapshot
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
      opp_snapshot.snapshot_day_of_fiscal_year_normalised,
  
      opp_snapshot.close_fiscal_year,
      opp_snapshot.created_fiscal_year,
      opp_snapshot.pipeline_created_fiscal_year,
      -----------------------------------------------------------------------------------
    
      opp_snapshot.stage_name,
      opp_snapshot.forecast_category_name,
      opp_snapshot.is_renewal,
      opp_snapshot.is_won,
      opp_snapshot.is_lost,
      opp_snapshot.is_open,
      opp_snapshot.is_excluded_flag,
     
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
 
  
   ), current_fiscal_year AS (
  
    SELECT 
      pipeline_snapshot.snapshot_fiscal_year                        AS close_fiscal_year,
      pipeline_snapshot.snapshot_day_of_fiscal_year_normalised      AS close_day_of_fiscal_year_normalised,
   
      
      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(pipeline_snapshot.deal_count)                           AS cfy_deal_count,
      SUM(pipeline_snapshot.booked_deal_count)                    AS cfy_booked_deal_count,
      SUM(pipeline_snapshot.churned_contraction_deal_count)       AS cfy_churned_contraction_deal_count,

      SUM(pipeline_snapshot.open_1plus_deal_count)                AS cfy_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                AS cfy_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                AS cfy_open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                   AS cfy_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                   AS cfy_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                   AS cfy_open_4plus_net_arr,
      SUM(pipeline_snapshot.booked_net_arr)                       AS cfy_booked_net_arr,
      
      -- churned net_arr
      SUM(pipeline_snapshot.churned_contraction_net_arr)          AS cfy_churned_contraction_net_arr
  
      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- snapshot quarter rows that close within the same quarter
    WHERE pipeline_snapshot.snapshot_fiscal_year = pipeline_snapshot.close_fiscal_year
    GROUP BY 1,2,3
  
  
 ), next_fiscal_year AS (
  
    SELECT 
      pipeline_snapshot.snapshot_fiscal_year                        AS close_fiscal_year,
      pipeline_snapshot.snapshot_day_of_fiscal_year_normalised      AS close_day_of_fiscal_year_normalised,
   
      pipeline_snapshot.close_fiscal_year                           AS nfy_close_fiscal_year,      
      
      -------------------
      -- report keys
      pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(pipeline_snapshot.deal_count)                           AS nfy_deal_count,
      SUM(pipeline_snapshot.booked_deal_count)                    AS nfy_booked_deal_count,
      SUM(pipeline_snapshot.churned_contraction_deal_count)       AS nfy_churned_contraction_deal_count,

      SUM(pipeline_snapshot.open_1plus_deal_count)                AS nfy_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                AS nfy_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                AS nfy_open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                   AS nfy_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                   AS nfy_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                   AS nfy_open_4plus_net_arr,
      SUM(pipeline_snapshot.booked_net_arr)                       AS nfy_booked_net_arr,
      
      -- churned net_arr
      SUM(pipeline_snapshot.churned_contraction_net_arr)          AS nfy_churned_contraction_net_arr
  
      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- snapshot quarter rows that close within the same quarter
    WHERE pipeline_snapshot.snapshot_fiscal_year = pipeline_snapshot.close_fiscal_year - 1
    GROUP BY 1,2,3,4
   
  
), pipeline_gen AS (

    SELECT
      opp_history.snapshot_fiscal_year                      AS close_fiscal_year,
      opp_history.snapshot_day_of_fiscal_year_normalised    AS close_day_of_fiscal_year_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opp_history.created_in_snapshot_quarter_deal_count)     AS pipe_gen_count,

      -- Net ARR 
      SUM(opp_history.created_in_snapshot_quarter_net_arr)        AS pipe_gen_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_year = opp_history.pipeline_created_fiscal_year
      AND opp_history.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2,3

--Sales Accepted Opportunities
), sao_gen AS (

    SELECT
     opp_history.snapshot_fiscal_year                      AS close_fiscal_year,
     opp_history.snapshot_day_of_fiscal_year_normalised    AS close_day_of_fiscal_year_normalised,

      -------------------
      -- report keys
      opp_history.report_user_segment_geo_region_area_sqs_ot,
      -------------------

      SUM(opp_history.calculated_deal_count)     AS sao_deal_count,

      -- Net ARR 
      SUM(opp_history.net_arr)                  AS sao_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_year = opp_history.sales_accepted_fiscal_year
      AND opp_history.is_eligible_sao_flag = 1
    GROUP BY 1,2,3
  

-- These CTE builds a complete set of values 
), key_fields AS (
  
 SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_year
  FROM current_fiscal_year
  UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_year
  FROM next_fiscal_year
   UNION
   SELECT
     report_user_segment_geo_region_area_sqs_ot,
     close_fiscal_year
  FROM sao_gen

), base_fields AS (
  
  SELECT 
      key_fields.*,
      close_date.day_of_fiscal_year_normalised AS close_day_of_fiscal_year_normalised
  FROM key_fields
  INNER JOIN date_details close_date
    ON close_date.fiscal_year = key_fields.close_fiscal_year

), report_pipeline_metrics_day AS (
      
    SELECT 
      -----------------------------
      -- keys
      base_fields.report_user_segment_geo_region_area_sqs_ot,

      base_fields.close_fiscal_year,
      base_fields.close_day_of_fiscal_year_normalised,
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

      ---------------------------------------------------
      -- current fiscal year
      COALESCE(current_fiscal_year.cfy_deal_count,0)                     AS cfy_deal_count,
      COALESCE(current_fiscal_year.cfy_open_1plus_deal_count,0)          AS cfy_open_1plus_deal_count,
      COALESCE(current_fiscal_year.cfy_open_3plus_deal_count,0)          AS cfy_open_3plus_deal_count,
      COALESCE(current_fiscal_year.cfy_open_4plus_deal_count,0)          AS cfy_open_4plus_deal_count, 
      COALESCE(current_fiscal_year.cfy_booked_deal_count,0)              AS cfy_booked_deal_count,
      -- churned deal count
      COALESCE(current_fiscal_year.cfy_churned_contraction_deal_count,0) AS cfy_churned_contraction_deal_count,
     
      COALESCE(current_fiscal_year.cfy_booked_net_arr,0)                 AS cfy_booked_net_arr,
      -- churned net_arr
      COALESCE(current_fiscal_year.cfy_churned_contraction_net_arr,0)    AS cfy_churned_contraction_net_arr,
      COALESCE(current_fiscal_year.cfy_open_1plus_net_arr,0)             AS cfy_open_1plus_net_arr,
      COALESCE(current_fiscal_year.cfy_open_3plus_net_arr,0)             AS cfy_open_3plus_net_arr, 
      COALESCE(current_fiscal_year.cfy_open_4plus_net_arr,0)             AS cfy_open_4plus_net_arr, 
      
      ---------------------------------------------------
      -- next fiscal year
      COALESCE(next_fiscal_year.nfy_deal_count,0)                     AS nfy_deal_count,
      COALESCE(next_fiscal_year.nfy_open_1plus_deal_count,0)          AS nfy_open_1plus_deal_count,
      COALESCE(next_fiscal_year.nfy_open_3plus_deal_count,0)          AS nfy_open_3plus_deal_count,
      COALESCE(next_fiscal_year.nfy_open_4plus_deal_count,0)          AS nfy_open_4plus_deal_count, 
      COALESCE(next_fiscal_year.nfy_booked_deal_count,0)              AS nfy_booked_deal_count,
      -- churned deal count
      COALESCE(next_fiscal_year.nfy_churned_contraction_deal_count,0) AS nfy_churned_contraction_deal_count,
     
      COALESCE(next_fiscal_year.nfy_booked_net_arr,0)                 AS nfy_booked_net_arr,
      -- churned net_arr
      COALESCE(next_fiscal_year.nfy_churned_contraction_net_arr,0)    AS nfy_churned_contraction_net_arr,
      COALESCE(next_fiscal_year.nfy_open_1plus_net_arr,0)             AS nfy_open_1plus_net_arr,
      COALESCE(next_fiscal_year.nfy_open_3plus_net_arr,0)             AS nfy_open_3plus_net_arr, 
      COALESCE(next_fiscal_year.nfy_open_4plus_net_arr,0)             AS nfy_open_4plus_net_arr, 
      ------------------------------
    
      -- pipe gen
      COALESCE(pipeline_gen.pipe_gen_count,0)                           AS cfy_pipe_gen_count,
      COALESCE(pipeline_gen.pipe_gen_net_arr,0)                         AS cfy_pipe_gen_net_arr,

       -- sao gen
      COALESCE(sao_gen.sao_deal_count,0)                                AS cfy_sao_deal_count,
      COALESCE(sao_gen.sao_net_arr,0)                                   AS cfy_sao_net_arr,

       -- one year ago sao gen
      COALESCE(minus_1_year_sao_gen.sao_net_arr,0)                      AS minus_1_year_fy_sao_net_arr,
      COALESCE(minus_1_year_sao_gen.sao_deal_count,0)                   AS minus_1_year_fy_sao_deal_count,

      -- one year ago pipe gen
      COALESCE(minus_1_year_pipe_gen.pipe_gen_net_arr,0)                AS minus_1_year_fy_pipe_gen_net_arr,
      COALESCE(minus_1_year_pipe_gen.pipe_gen_count,0)                  AS minus_1_year_fy_pipe_gen_deal_count,

      -- TIMESTAMP
      current_timestamp                                                 AS dbt_last_run_at

    -- created a list of all options to avoid having blanks when attaching metrics
    FROM base_fields
    -- base keys dictionary
    LEFT JOIN agg_demo_keys
      ON base_fields.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    -- historical quarter
    LEFT JOIN current_fiscal_year
      ON base_fields.close_day_of_fiscal_year_normalised = current_fiscal_year.close_day_of_fiscal_year_normalised
      AND base_fields.close_fiscal_year = current_fiscal_year.close_fiscal_year   
      AND base_fields.report_user_segment_geo_region_area_sqs_ot = current_fiscal_year.report_user_segment_geo_region_area_sqs_ot
    -- next quarter in relation to the considered reported quarter
    LEFT JOIN  next_fiscal_year
      ON base_fields.close_day_of_fiscal_year_normalised = next_fiscal_year.close_day_of_fiscal_year_normalised
        AND base_fields.close_fiscal_year = next_fiscal_year.close_fiscal_year   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = next_fiscal_year.report_user_segment_geo_region_area_sqs_ot    
    -- Pipe generation piece
    LEFT JOIN pipeline_gen 
      ON base_fields.close_day_of_fiscal_year_normalised = pipeline_gen.close_day_of_fiscal_year_normalised
        AND base_fields.close_fiscal_year = pipeline_gen.close_fiscal_year   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = pipeline_gen.report_user_segment_geo_region_area_sqs_ot
    -- Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen
       ON base_fields.close_day_of_fiscal_year_normalised = sao_gen.close_day_of_fiscal_year_normalised
        AND base_fields.close_fiscal_year = sao_gen.close_fiscal_year   
        AND base_fields.report_user_segment_geo_region_area_sqs_ot = sao_gen.report_user_segment_geo_region_area_sqs_ot
    -- One Year Ago  pipeline generation
    LEFT JOIN pipeline_gen  minus_1_year_pipe_gen
      ON minus_1_year_pipe_gen.close_day_of_fiscal_year_normalised = base_fields.close_day_of_fiscal_year_normalised
        AND minus_1_year_pipe_gen.close_fiscal_year = base_fields.close_fiscal_year - 1
        AND minus_1_year_pipe_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot
    -- One Year Ago Sales Accepted Opportunity Generation
    LEFT JOIN sao_gen minus_1_year_sao_gen
      ON minus_1_year_sao_gen.close_day_of_fiscal_year_normalised = base_fields.close_day_of_fiscal_year_normalised
        AND minus_1_year_sao_gen.close_fiscal_year = base_fields.close_fiscal_year - 1
        AND minus_1_year_sao_gen.report_user_segment_geo_region_area_sqs_ot = base_fields.report_user_segment_geo_region_area_sqs_ot

)

SELECT *
FROM report_pipeline_metrics_day