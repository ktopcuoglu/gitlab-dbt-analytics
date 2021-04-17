{{ config(alias='report_pipeline_metrics_day') }}

WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}  

), sfdc_opportunity_xf AS (

    SELECT 
        opties.*,
        today_date.date_actual                      AS snapshot_date,
        today_date.day_of_fiscal_quarter_normalised AS snapshot_day_of_fiscal_quarter_normalised,
        today_date.fiscal_quarter_name_fy           AS snapshot_fiscal_quarter_name,
        today_date.first_day_of_fiscal_quarter      AS snapshot_fiscal_quarter_date,
        today_date.fiscal_year                      AS snapshot_fiscal_year
        
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}} opties
    CROSS JOIN (SELECT *
                  FROM date_details
                  WHERE date_actual = CURRENT_DATE) today_date 
    WHERE opties.is_deleted = 0
      AND opties.is_excluded_flag = 0
      AND opties.is_edu_oss = 0
      AND opties.net_arr is not null
      AND lower(opties.deal_group) LIKE ANY ('%growth%', '%new%')
      AND ((opties.forecast_category_name != 'Omitted'
              AND opties.is_stage_1_plus = 1)
            OR opties.is_lost = 1)     
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
      AND ((opp_snapshot.forecast_category_name != 'Omitted'
              AND opp_snapshot.is_stage_1_plus = 1)
            OR opp_snapshot.is_lost = 1)    
       -- Not JiHu account
 
), pipeline_snapshot AS (

    SELECT 
      -------------------------------------
      -- report keys
      COALESCE(opp_snapshot.sales_team_cro_level,'NA')    AS sales_team_cro_level,
      COALESCE(opp_snapshot.sales_team_rd_asm_level,'NA') AS sales_team_rd_asm_level,
      COALESCE(opp_snapshot.sales_qualified_source,'NA')  AS sales_qualified_source,
      COALESCE(opp_snapshot.deal_category,'NA')           AS deal_category,
      COALESCE(opp_snapshot.deal_group,'NA')              AS deal_group,
      COALESCE(opp_snapshot.owner_id,'NA')                AS owner_id,
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
  
      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      opp_snapshot.open_1plus_net_arr,
      opp_snapshot.open_3plus_net_arr,
      opp_snapshot.open_4plus_net_arr,
  
      -- booked net _arr
      opp_snapshot.booked_net_arr,
  
      opp_snapshot.created_and_won_same_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
    -- Keep the UNION ALL, somehow UNION is making us loose data
    UNION ALL
    SELECT 
      -------------------------------------
      -- report keys
      COALESCE(opties.sales_team_cro_level,'NA')    AS sales_team_cro_level,
      COALESCE(opties.sales_team_rd_asm_level,'NA') AS sales_team_rd_asm_level,
      COALESCE(opties.sales_qualified_source,'NA')  AS sales_qualified_source,
      COALESCE(opties.deal_category,'NA')           AS deal_category,
      COALESCE(opties.deal_group,'NA')              AS deal_group,
      COALESCE(opties.owner_id,'NA')                AS owner_id,
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
  
      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      opties.open_1plus_net_arr,
      opties.open_3plus_net_arr,
      opties.open_4plus_net_arr,
  
      -- booked net _arr
      opties.booked_net_arr,
  
      -- created and closed within the quarter net arr
      opties.created_and_won_same_quarter_net_arr,
      
      -- created within quarter
      CASE
        WHEN opties.pipeline_created_fiscal_quarter_name = opties.snapshot_fiscal_quarter_name
          AND opties.is_eligible_created_pipeline_flag = 1
            THEN opties.net_arr
        ELSE 0 
      END                                                AS created_in_snapshot_quarter_net_arr
  

    FROM sfdc_opportunity_xf opties
  
), reported_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT 
      pipeline_snapshot.close_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised,    
      
      -------------------
      -- report keys
      pipeline_snapshot.sales_team_cro_level,
      pipeline_snapshot.sales_team_rd_asm_level,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,
      pipeline_snapshot.sales_qualified_source,
      pipeline_snapshot.owner_id,
      -------------------
      SUM(pipeline_snapshot.deal_count)                           AS deal_count,
      SUM(pipeline_snapshot.booked_deal_count)                    AS booked_deal_count,
      SUM(pipeline_snapshot.open_1plus_deal_count)                AS open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                AS open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                AS open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                   AS open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                   AS open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                   AS open_4plus_net_arr,
      SUM(pipeline_snapshot.booked_net_arr)                       AS booked_net_arr,
  
      SUM(pipeline_snapshot.created_and_won_same_quarter_net_arr) AS created_and_won_same_quarter_net_arr

      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- snapshot quarter rows that close within the same quarter
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_name = pipeline_snapshot.close_fiscal_quarter_name
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  
-- Quarter plus 1, from the reported quarter perspective
), report_quarter_plus_1 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      pipeline_snapshot.close_fiscal_quarter_name         AS rq_plus_1_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date         AS rq_plus_1_close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised,  
 
      -------------------
      -- report keys
      pipeline_snapshot.sales_team_cro_level,
      pipeline_snapshot.sales_team_rd_asm_level,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,
      pipeline_snapshot.sales_qualified_source,
      pipeline_snapshot.owner_id,
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
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  
-- Quarter plus 2, from the reported quarter perspective
), report_quarter_plus_2 AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      pipeline_snapshot.close_fiscal_quarter_name            AS rq_plus_2_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date            AS rq_plus_2_close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised,  
 
      -------------------
      -- report keys
      pipeline_snapshot.sales_team_cro_level,
      pipeline_snapshot.sales_team_rd_asm_level,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,
      pipeline_snapshot.sales_qualified_source,
      pipeline_snapshot.owner_id,
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
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  
), pipeline_gen AS (

    SELECT
      opp_history.snapshot_fiscal_quarter_date,
      opp_history.snapshot_day_of_fiscal_quarter_normalised,

      -------------------
      -- report keys
      opp_history.sales_team_cro_level,
      opp_history.sales_team_rd_asm_level,
      opp_history.deal_category, 
      opp_history.deal_group,
      opp_history.sales_qualified_source,
      opp_history.owner_id,
      -------------------

      SUM(opp_history.calculated_deal_count)     AS created_in_quarter_count,

      -- Net ARR 
      SUM(opp_history.net_arr)                   AS created_in_quarter_net_arr

    FROM sfdc_opportunity_snapshot_history_xf opp_history
    -- restrict the rows to pipeline created on the quarter of the snapshot
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.pipeline_created_fiscal_quarter_name
    AND opp_history.is_eligible_created_pipeline_flag = 1
    AND net_arr IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8

-- These CTE builds a complete set of values 
), key_fields AS (
  
 SELECT
     sales_team_cro_level, 
     sales_team_rd_asm_level,
     deal_category,
     deal_group,
     sales_qualified_source,
     owner_id,  
     snapshot_fiscal_quarter_date
  FROM reported_quarter
  UNION
   SELECT
     sales_team_cro_level, 
     sales_team_rd_asm_level,
     deal_category,
     deal_group,
     sales_qualified_source,
     owner_id,  
     snapshot_fiscal_quarter_date
  FROM report_quarter_plus_1
  UNION
   SELECT
     sales_team_cro_level, 
     sales_team_rd_asm_level,
     deal_category,
     deal_group,
     sales_qualified_source,
     owner_id,  
     snapshot_fiscal_quarter_date
  FROM report_quarter_plus_2
  UNION
   SELECT
     sales_team_cro_level, 
     sales_team_rd_asm_level,
     deal_category,
     deal_group,
     sales_qualified_source,
     owner_id,  
     snapshot_fiscal_quarter_date
  FROM pipeline_gen

), base_fields AS (
  
  SELECT key_fields.*,
      date_details.fiscal_quarter_name_fy           AS snapshot_fiscal_quarter_name,
      date_details.day_of_fiscal_quarter_normalised AS snapshot_day_of_fiscal_quarter_normalised,
      date_details.fiscal_year                      AS snapshot_fiscal_year
  FROM key_fields
  INNER JOIN date_details
    ON date_details.first_day_of_fiscal_quarter = key_fields.snapshot_fiscal_quarter_date

), report_pipeline_metrics_day AS (
      
    SELECT 
      -----------------------------
      -- keys
      base_fields.sales_team_cro_level, 
      base_fields.sales_team_rd_asm_level,
      base_fields.deal_category,
      base_fields.deal_group,
      base_fields.sales_qualified_source,
      base_fields.owner_id,
      -----------------------------

      base_fields.snapshot_fiscal_quarter_date                    AS close_fiscal_quarter_date,
      base_fields.snapshot_fiscal_quarter_name                    AS close_fiscal_quarter_name,
      base_fields.snapshot_fiscal_quarter_date,
      base_fields.snapshot_fiscal_quarter_name,
      base_fields.snapshot_fiscal_year,
      base_fields.snapshot_day_of_fiscal_quarter_normalised,

      -- report quarter plus 1 / 2 date fields
      report_quarter_plus_1.rq_plus_1_close_fiscal_quarter_name,
      report_quarter_plus_1.rq_plus_1_close_fiscal_quarter_date,
      report_quarter_plus_2.rq_plus_2_close_fiscal_quarter_name,
      report_quarter_plus_2.rq_plus_2_close_fiscal_quarter_date,    
  
      -- reported quarter
      COALESCE(reported_quarter.deal_count,0)                     AS deal_count,
      COALESCE(reported_quarter.open_1plus_deal_count,0)          AS open_1plus_deal_count,
      COALESCE(reported_quarter.open_3plus_deal_count,0)          AS open_3plus_deal_count,
      COALESCE(reported_quarter.open_4plus_deal_count,0)          AS open_4plus_deal_count, 
      COALESCE(reported_quarter.booked_deal_count,0)              AS booked_deal_count,
     
      COALESCE(pipeline_gen.created_in_quarter_count,0)           AS created_in_quarter_count,

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
      COALESCE(reported_quarter.open_1plus_net_arr,0)             AS open_1plus_net_arr,
      COALESCE(reported_quarter.open_3plus_net_arr,0)             AS open_3plus_net_arr, 
      COALESCE(reported_quarter.open_4plus_net_arr,0)             AS open_4plus_net_arr, 

      COALESCE(reported_quarter.created_and_won_same_quarter_net_arr,0) AS created_and_won_same_quarter_net_arr,
      COALESCE(pipeline_gen.created_in_quarter_net_arr,0)               AS created_in_quarter_net_arr,

        -- reported quarter + 1
      COALESCE(report_quarter_plus_1.rq_plus_1_open_1plus_net_arr,0)       AS rq_plus_1_open_1plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_3plus_net_arr,0)       AS rq_plus_1_open_3plus_net_arr,
      COALESCE(report_quarter_plus_1.rq_plus_1_open_4plus_net_arr,0)       AS rq_plus_1_open_4plus_net_arr,

      -- reported quarter + 1
      COALESCE(report_quarter_plus_2.rq_plus_2_open_1plus_net_arr,0)       AS rq_plus_2_open_1plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_3plus_net_arr,0)       AS rq_plus_2_open_3plus_net_arr,
      COALESCE(report_quarter_plus_2.rq_plus_2_open_4plus_net_arr,0)       AS rq_plus_2_open_4plus_net_arr

    -- created a list of all options to avoid having blanks when attaching metrics
    FROM base_fields
    -- historical quarter
    LEFT JOIN reported_quarter
      ON base_fields.sales_team_cro_level = reported_quarter.sales_team_cro_level
      AND base_fields.sales_team_rd_asm_level = reported_quarter.sales_team_rd_asm_level
      AND base_fields.snapshot_fiscal_quarter_date = reported_quarter.snapshot_fiscal_quarter_date
      AND base_fields.deal_category = reported_quarter.deal_category
      AND base_fields.snapshot_day_of_fiscal_quarter_normalised = reported_quarter.snapshot_day_of_fiscal_quarter_normalised
      AND base_fields.sales_qualified_source = reported_quarter.sales_qualified_source
      AND base_fields.deal_group = reported_quarter.deal_group
      AND base_fields.owner_id = reported_quarter.owner_id
   
    -- next quarter in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_1
      ON report_quarter_plus_1.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
      AND report_quarter_plus_1.sales_team_rd_asm_level = base_fields.sales_team_rd_asm_level
      AND report_quarter_plus_1.sales_team_cro_level = base_fields.sales_team_cro_level
      AND report_quarter_plus_1.deal_category = base_fields.deal_category
      AND report_quarter_plus_1.snapshot_day_of_fiscal_quarter_normalised = base_fields.snapshot_day_of_fiscal_quarter_normalised
      AND report_quarter_plus_1.sales_qualified_source = base_fields.sales_qualified_source
      AND report_quarter_plus_1.deal_group = base_fields.deal_group  
      AND report_quarter_plus_1.owner_id = base_fields.owner_id
    
    -- 2 quarters ahead in relation to the considered reported quarter
    LEFT JOIN  report_quarter_plus_2
      ON report_quarter_plus_2.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
      AND report_quarter_plus_2.sales_team_rd_asm_level = base_fields.sales_team_rd_asm_level
      AND report_quarter_plus_2.sales_team_cro_level = base_fields.sales_team_cro_level
      AND report_quarter_plus_2.deal_category = base_fields.deal_category
      AND report_quarter_plus_2.snapshot_day_of_fiscal_quarter_normalised = base_fields.snapshot_day_of_fiscal_quarter_normalised
      AND report_quarter_plus_2.sales_qualified_source = base_fields.sales_qualified_source
      AND report_quarter_plus_2.deal_group = base_fields.deal_group
      AND report_quarter_plus_2.owner_id = base_fields.owner_id
    
    -- Pipe generation piece
    LEFT JOIN pipeline_gen 
      ON pipeline_gen.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
      AND pipeline_gen.snapshot_day_of_fiscal_quarter_normalised = base_fields.snapshot_day_of_fiscal_quarter_normalised        
      AND pipeline_gen.sales_team_rd_asm_level = base_fields.sales_team_rd_asm_level
      AND pipeline_gen.sales_team_cro_level = base_fields.sales_team_cro_level
      AND pipeline_gen.deal_category = base_fields.deal_category
      AND pipeline_gen.sales_qualified_source = base_fields.sales_qualified_source
      AND pipeline_gen.deal_group = base_fields.deal_group
      AND pipeline_gen.owner_id = base_fields.owner_id
)

SELECT *
FROM report_pipeline_metrics_day
