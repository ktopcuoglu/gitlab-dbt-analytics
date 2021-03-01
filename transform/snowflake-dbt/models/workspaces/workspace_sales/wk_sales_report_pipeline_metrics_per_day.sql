{{ config(alias='report_pipeline_metrics_day', materialized='table') }}

WITH date_details AS (

    SELECT
      *,
      90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS day_of_fiscal_quarter_normalised,
      12-floor((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)/7))  AS week_of_fiscal_quarter_normalised,
      CASE 
        WHEN ((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)-6) % 7 = 0 
                OR date_actual = first_day_of_fiscal_quarter) 
          THEN 1 
          ELSE 0 
      END                                                                   AS first_day_of_fiscal_quarter_week_normalised 
    FROM {{ ref('date_details') }} 
    ORDER BY 1 DESC

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  
), sfdc_opportunity_snapshot_history_xf_restricted AS (
    
    SELECT *
    FROM sfdc_opportunity_snapshot_history_xf
      CROSS JOIN (SELECT *
                  FROM date_details
                  WHERE date_actual = DATEADD(day,-1,CURRENT_DATE)) today_date 
    -- remove lost & deleted deals
    WHERE
      -- remove excluded deals
      is_excluded_flag = 0
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      AND (forecast_category_name != 'Omitted'
         OR is_lost = 1)
      AND snapshot_fiscal_quarter_name != today_date.fiscal_quarter_name_fy 
  
  
--NF: Is this accounting correctly for Churn?
), pipeline_snapshot AS (

    SELECT 
      -------------------------------------
      -- report keys
      opp_snapshot.sales_team_cro_level,
      opp_snapshot.sales_team_rd_asm_level,
      opp_snapshot.sales_qualified_source,
      opp_snapshot.deal_category,
      opp_snapshot.deal_group,
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

      opp_snapshot.incremental_acv,
      opp_snapshot.net_incremental_acv,
      opp_snapshot.net_arr,

      opp_snapshot.calculated_deal_count                             AS deal_count,

      CASE 
        WHEN opp_snapshot.is_open = 1
          AND opp_snapshot.is_stage_1_plus = 1
          THEN opp_snapshot.calculated_deal_count  
        ELSE 0                                                                                              
      END                                                            AS open_1plus_deal_count,

      CASE 
        WHEN opp_snapshot.is_open = 1
         AND opp_snapshot.is_stage_3_plus = 1
          THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                                            AS open_3plus_deal_count,

      CASE 
        WHEN opp_snapshot.is_open = 1
          AND opp_snapshot.is_stage_4_plus = 1
          THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                                            AS open_4plus_deal_count,

      CASE 
        WHEN opp_snapshot.is_won = 1
          THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                                            AS won_deal_count,
 
      -----------------------------------------------------------------------------------
      -- NF: 20210201 DEPRECATED IACV fields
      
      CASE 
        WHEN opp_snapshot.is_open = 1
            AND opp_snapshot.is_stage_1_plus = 1  
          THEN opp_snapshot.incremental_acv
        ELSE 0                                                                                              
      END                                                            AS open_1plus_iacv,

        CASE 
        WHEN opp_snapshot.is_open = 1
          AND opp_snapshot.is_stage_3_plus = 1   
            THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                             AS open_3plus_iacv,
  
      CASE 
        WHEN opp_snapshot.is_open = 1    
          AND opp_snapshot.is_stage_4_plus = 1
          THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                             AS open_4plus_iacv,

      CASE 
       WHEN opp_snapshot.is_won = 1 
          THEN opp_snapshot.incremental_acv
        ELSE 0  
      END                                                             AS won_iacv,
  
      CASE
        WHEN (opp_snapshot.is_won = 1 
          OR (opp_snapshot.is_renewal = 1 AND opp_snapshot.is_lost = 1))
          THEN opp_snapshot.net_incremental_acv  
        ELSE 0
      END                                                              AS won_net_iacv,

      opp_snapshot.created_and_won_same_quarter_iacv                   AS created_and_won_iacv,
      opp_snapshot.created_in_snapshot_quarter_iacv                    AS created_in_quarter_iacv,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      CASE 
        WHEN  opp_snapshot.is_open = 1
            AND opp_snapshot.is_stage_1_plus = 1  
          THEN opp_snapshot.net_arr
        ELSE 0                                                                                              
      END                                                               AS open_1plus_net_arr,

      CASE 
        WHEN opp_snapshot.is_open = 1
          AND opp_snapshot.is_stage_3_plus = 1   
          THEN opp_snapshot.net_arr
        ELSE 0
      END                                                                AS open_3plus_net_arr,
  
      CASE 
        WHEN opp_snapshot.is_open = 1    
          AND opp_snapshot.is_stage_4_plus = 1
          THEN opp_snapshot.net_arr
        ELSE 0
      END                                                                AS open_4plus_net_arr,

      CASE
        WHEN (opp_snapshot.is_won = 1 
          OR (opp_snapshot.is_renewal = 1 AND opp_snapshot.is_lost = 1))
          THEN opp_snapshot.net_arr  
        ELSE 0
      END                                                                AS won_net_arr,
      opp_snapshot.created_and_won_same_quarter_net_arr,
      opp_snapshot.created_in_snapshot_quarter_net_arr

    FROM sfdc_opportunity_snapshot_history_xf_restricted opp_snapshot
    -- till end of quarter
    WHERE       
      opp_snapshot.snapshot_date <= DATEADD(month,3,opp_snapshot.close_fiscal_quarter_date)
      -- 1 quarters before start
      AND opp_snapshot.snapshot_date >= DATEADD(month,-3,opp_snapshot.close_fiscal_quarter_date)
     
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
      -------------------
        
      SUM(pipeline_snapshot.won_deal_count)                                           AS won_deal_count,
      SUM(pipeline_snapshot.open_1plus_deal_count)                                    AS open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                                    AS open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                                    AS open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201 DEPRECATED IACV fields   
      -- open / won pipeline in quarter
      
      SUM(pipeline_snapshot.open_1plus_iacv)                                          AS open_1plus_iacv,
      SUM(pipeline_snapshot.open_3plus_iacv)                                          AS open_3plus_iacv,
      SUM(pipeline_snapshot.open_4plus_iacv)                                          AS open_4plus_iacv,
      SUM(pipeline_snapshot.won_iacv)                                                 AS won_iacv,
      SUM(pipeline_snapshot.won_net_iacv)                                             AS won_net_iacv,
      SUM(pipeline_snapshot.created_and_won_iacv)                                     AS created_and_won_iacv,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_1plus_net_arr)                                       AS open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                                       AS open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                                       AS open_4plus_net_arr,
      SUM(pipeline_snapshot.won_net_arr)                                              AS won_net_arr,
      SUM(pipeline_snapshot.created_and_won_same_quarter_net_arr)                     AS created_and_won_same_quarter_net_arr

      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- restrict the rows to pipeline of the quarter the snapshot was taken
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_name = pipeline_snapshot.close_fiscal_quarter_name
    -- to account for net iacv, it is needed to include lost renewal deals
    AND (pipeline_snapshot.is_lost = 0
      OR (pipeline_snapshot.is_renewal = 1 AND pipeline_snapshot.is_lost = 1))   
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  
), next_quarter AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      pipeline_snapshot.close_fiscal_quarter_name                                  AS next_close_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date                                  AS next_close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised,  
 
  -------------------
      -- report keys
      pipeline_snapshot.sales_team_cro_level,
      pipeline_snapshot.sales_team_rd_asm_level,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,
      pipeline_snapshot.sales_qualified_source,
      -------------------
     
      SUM(pipeline_snapshot.open_1plus_deal_count)                             AS next_open_1plus_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                             AS next_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                             AS next_open_4plus_deal_count,

      ------------------------------
      -- DEPRECATED IACV METRICS

      SUM(pipeline_snapshot.open_1plus_iacv)                                   AS next_open_1plus_iacv,
      SUM(pipeline_snapshot.open_3plus_iacv)                                   AS next_open_3plus_iacv,
      SUM(pipeline_snapshot.open_4plus_iacv)                                   AS next_open_4plus_iacv,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead

      SUM(pipeline_snapshot.open_1plus_net_arr)                                 AS next_open_1plus_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                                 AS next_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                                 AS next_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show next quarter lines
    -- without this we would get results for multiple quarters
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -3,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND pipeline_snapshot.is_lost = 0  
    GROUP BY 1,2,3,4,5,6,7,8,9,10

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
      -------------------

      SUM(opp_history.calculated_deal_count)     AS created_in_quarter_count,

      ------------------------------
      -- DEPRECATED IACV METRICS
      SUM(opp_history.incremental_acv)           AS created_in_quarter_iacv,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead      
      SUM(opp_history.net_arr)                   AS created_in_quarter_net_arr

    FROM sfdc_opportunity_snapshot_history_xf_restricted opp_history
    -- restrict the rows to pipeline of the quarter the snapshot was taken
    WHERE opp_history.snapshot_fiscal_quarter_name = opp_history.pipeline_created_fiscal_quarter_name
      -- remove pre-opty deals
      -- remove pre-opty deals and stage 0
      AND opp_history.stage_name NOT IN ('0-Pending Acceptance','10-Duplicate','00-Pre Opportunity','9-Unqualified')
    GROUP BY 1,2,3,4,5,6,7

), base_fields AS (
    
    SELECT DISTINCT 
      -----------------------------
      -- keys
      a.sales_team_cro_level,
      a.sales_team_rd_asm_level,
      b.deal_category,
      b.deal_group,
      -----------------------------
      f.sales_qualified_source,
      c.snapshot_fiscal_quarter_date,
      d.snapshot_fiscal_quarter_name,
      d.snapshot_day_of_fiscal_quarter_normalised,
      d.snapshot_next_fiscal_quarter_date
    FROM (SELECT DISTINCT sales_team_cro_level
                    , sales_team_rd_asm_level FROM pipeline_snapshot) a
    CROSS JOIN (SELECT DISTINCT deal_category,
                                deal_group 
                FROM pipeline_snapshot) b
    CROSS JOIN (SELECT DISTINCT snapshot_fiscal_quarter_date FROM pipeline_snapshot) c
    CROSS JOIN (SELECT DISTINCT sales_qualified_source FROM pipeline_snapshot) f
    INNER JOIN (SELECT DISTINCT fiscal_quarter_name_fy                                                              AS snapshot_fiscal_quarter_name,
                              first_day_of_fiscal_quarter                                                           AS snapshot_fiscal_quarter_date, 
                              DATEADD(month,3,first_day_of_fiscal_quarter)                                          AS snapshot_next_fiscal_quarter_date,
                              day_of_fiscal_quarter_normalised                                                      AS snapshot_day_of_fiscal_quarter_normalised
              FROM date_details) d
      ON c.snapshot_fiscal_quarter_date = d.snapshot_fiscal_quarter_date 
), report_pipeline_metrics_day AS (
      
    SELECT 
      -----------------------------
      -- keys
      base_fields.sales_team_cro_level, 
      base_fields.sales_team_rd_asm_level,
      base_fields.deal_category,
      base_fields.deal_group,
      base_fields.sales_qualified_source,
      -----------------------------
      
      base_fields.snapshot_fiscal_quarter_name                    AS close_fiscal_quarter_name,
      base_fields.snapshot_fiscal_quarter_name,
      
      base_fields.snapshot_fiscal_quarter_date                    AS close_fiscal_quarter_date,
      base_fields.snapshot_fiscal_quarter_date,
      
      base_fields.snapshot_day_of_fiscal_quarter_normalised,

      COALESCE(reported_quarter.open_1plus_deal_count,0)          AS open_1plus_deal_count,
      COALESCE(reported_quarter.open_3plus_deal_count,0)          AS open_3plus_deal_count,
      COALESCE(reported_quarter.open_4plus_deal_count,0)          AS open_4plus_deal_count, 
      COALESCE(reported_quarter.won_deal_count,0)                 AS won_deal_count,
      pipeline_gen.created_in_quarter_count,

      COALESCE(next_quarter.next_open_1plus_deal_count,0)         AS next_open_1plus_deal_count,
      COALESCE(next_quarter.next_open_3plus_deal_count,0)         AS next_open_3plus_deal_count,
      COALESCE(next_quarter.next_open_4plus_deal_count,0)         AS next_open_4plus_deal_count,

      ------------------------------
      -- DEPRECATED IACV METRICS  

      COALESCE(reported_quarter.won_net_iacv,0)                   AS won_net_iacv,
      COALESCE(reported_quarter.won_iacv,0)                       AS won_iacv,
      COALESCE(reported_quarter.open_1plus_iacv,0)                AS open_1plus_iacv,
      COALESCE(reported_quarter.open_3plus_iacv,0)                AS open_3plus_pipeline_iacv, 
      COALESCE(reported_quarter.open_4plus_iacv,0)                AS open_4plus_pipeline_iacv, 

      reported_quarter.created_and_won_iacv,
      pipeline_gen.created_in_quarter_iacv,

      COALESCE(next_quarter.next_open_1plus_iacv,0)               AS next_open_iacv,
      COALESCE(next_quarter.next_open_3plus_iacv,0)               AS next_open_3plus_iacv,
      COALESCE(next_quarter.next_open_4plus_iacv,0)               AS next_open_4plus_iacv,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead     
      -- created and closed
      
      COALESCE(reported_quarter.won_net_arr,0)                    AS won_net_arr,
      COALESCE(reported_quarter.open_1plus_net_arr,0)             AS open_1plus_net_arr,
      COALESCE(reported_quarter.open_3plus_net_arr,0)             AS open_3plus_net_arr, 
      COALESCE(reported_quarter.open_4plus_net_arr,0)             AS open_4plus_net_arr, 

      reported_quarter.created_and_won_same_quarter_net_arr,
      pipeline_gen.created_in_quarter_net_arr,

      COALESCE(next_quarter.next_open_1plus_net_arr,0)            AS next_open_1plus_net_arr,
      COALESCE(next_quarter.next_open_3plus_net_arr,0)            AS next_open_3plus_net_arr,
      COALESCE(next_quarter.next_open_4plus_net_arr,0)            AS next_open_4plus_net_arr,

      -- next quarter 
      next_quarter_date.fiscal_quarter_name_fy                    AS next_close_fiscal_quarter_name,
      next_quarter_date.first_day_of_fiscal_quarter               AS next_close_fiscal_quarter_date                   


    -- created a list of all options to avoid having blanks when attaching totals in the reporting phase
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
    -- next quarter in relation to the considered reported quarter
    LEFT JOIN  next_quarter
      ON next_quarter.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
      AND next_quarter.sales_team_rd_asm_level = base_fields.sales_team_rd_asm_level
      AND next_quarter.sales_team_cro_level = base_fields.sales_team_cro_level
      AND next_quarter.deal_category = base_fields.deal_category
      AND next_quarter.snapshot_day_of_fiscal_quarter_normalised = base_fields.snapshot_day_of_fiscal_quarter_normalised
      AND next_quarter.sales_qualified_source = base_fields.sales_qualified_source
      AND next_quarter.deal_group = base_fields.deal_group
    LEFT JOIN pipeline_gen 
      ON pipeline_gen.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
      AND pipeline_gen.sales_team_rd_asm_level = base_fields.sales_team_rd_asm_level
      AND pipeline_gen.sales_team_cro_level = base_fields.sales_team_cro_level
      AND pipeline_gen.deal_category = base_fields.deal_category
      AND pipeline_gen.snapshot_day_of_fiscal_quarter_normalised = base_fields.snapshot_day_of_fiscal_quarter_normalised
      AND pipeline_gen.sales_qualified_source = base_fields.sales_qualified_source
      AND pipeline_gen.deal_group = base_fields.deal_group
    LEFT JOIN date_details next_quarter_date
      ON next_quarter_date.date_actual = base_fields.snapshot_next_fiscal_quarter_date
)


SELECT *
FROM report_pipeline_metrics_day