{{ config(alias='report_pipeline_metrics_day') }}

/*
TODO:
- Refactor the fields open_won, create fields for open and for won pipeline
- 
*/
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
    -- remove lost & deleted deals
    WHERE
      -- remove excluded deals
      is_excluded_flag = 0
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      AND (forecast_category_name != 'Omitted'
         OR lower(stage_name) LIKE '%lost%')
  
), pipeline_snapshot_base AS (
    
    SELECT
      -----------------
      -- report keys
      opportunity_owner_user_segment,
      COALESCE(sales_qualified_source, 'n/a')               AS sales_qualified_source,
      deal_category,
      deal_group,
    
      -- the account hierarchy can be related to the VP / ASM / RD levels
      -- and to an approximate region
      opportunity_owner_rd_asm_level,

      -----------------

      close_fiscal_quarter_name,
      close_fiscal_quarter_date,
      close_fiscal_year,

      snapshot_date,
      snapshot_date_month,
      snapshot_fiscal_year,
      snapshot_fiscal_quarter_name,
      snapshot_fiscal_quarter_date,
      snapshot_day_of_fiscal_quarter_normalised      AS snapshot_day_of_fiscal_quarter,

      created_fiscal_quarter_name,
      created_fiscal_quarter_date,
      created_fiscal_year,

      stage_name_3plus,
      stage_name_4plus,
      is_excluded_flag,
      stage_name,
      forecast_category_name,
      order_type_stamped,
      is_renewal,
      is_won,
      is_lost,
      is_open,

      -- sales team - region fields
      account_owner_team_stamped,
      account_owner_team_vp_level,
      account_owner_team_rd_level,
      account_owner_team_asm_level,
      account_owner_sales_region,

      SUM(calculated_deal_count)                      AS opps,
      SUM(incremental_acv)                            AS incremental_acv,
      SUM(net_incremental_acv)                        AS net_iacv,

      SUM(total_contract_value)                       AS tcv,

      SUM(created_in_snapshot_quarter_iacv)           AS created_in_quarter_iacv,
      SUM(created_and_won_same_quarter_iacv)          AS created_and_won_iacv,
      
      SUM(net_arr)                                    AS net_arr,
      SUM(created_in_snapshot_quarter_net_arr)        AS created_in_snapshot_quarter_net_arr,
      SUM(created_and_won_same_quarter_net_arr)       AS created_and_won_same_quarter_net_arr
      

    FROM sfdc_opportunity_snapshot_history_xf_restricted
     -- current day
    CROSS JOIN (SELECT *
                  FROM date_details
                  WHERE date_actual = DATEADD(day,-1,CURRENT_DATE)) today_date 
      -- exclude current quarter
    WHERE snapshot_fiscal_quarter_name != today_date.fiscal_quarter_name_fy 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32

--NF: Is this accounting correctly for Churn?
), pipeline_snapshot AS (

    SELECT 
      -------------------------------------
      -- report keys
      pipeline_snapshot_base.opportunity_owner_user_segment,
      pipeline_snapshot_base.opportunity_owner_rd_asm_level,
      pipeline_snapshot_base.sales_qualified_source,
      pipeline_snapshot_base.deal_category,
      pipeline_snapshot_base.deal_group,
      -------------------------------------
      
      pipeline_snapshot_base.stage_name,
      pipeline_snapshot_base.forecast_category_name,
      pipeline_snapshot_base.is_renewal,
      pipeline_snapshot_base.is_won,
      pipeline_snapshot_base.is_lost,
      pipeline_snapshot_base.is_open,
      
      pipeline_snapshot_base.is_excluded_flag,
      
      pipeline_snapshot_base.close_fiscal_quarter_name,
      pipeline_snapshot_base.close_fiscal_quarter_date,
      pipeline_snapshot_base.created_fiscal_quarter_name,
      pipeline_snapshot_base.created_fiscal_quarter_date,

      pipeline_snapshot_base.incremental_acv,
      pipeline_snapshot_base.net_iacv,
      pipeline_snapshot_base.net_arr,

      pipeline_snapshot_base.opps                                                                           AS deal_count,

      CASE 
        WHEN pipeline_snapshot_base.is_open = 1
          THEN pipeline_snapshot_base.opps  
        ELSE 0                                                                                              
      END                                                                                                   AS open_deal_count,

      CASE 
        WHEN pipeline_snapshot_base.stage_name_3plus IN ('3+ Pipeline')
          THEN pipeline_snapshot_base.opps
        ELSE 0
      END                                                                                                   AS open_3plus_deal_count,

      CASE 
        WHEN pipeline_snapshot_base.stage_name_4plus IN ('4+ Pipeline')
          THEN pipeline_snapshot_base.opps
        ELSE 0
      END                                                                                                   AS open_4plus_deal_count,

      CASE 
        WHEN pipeline_snapshot_base.is_won = 1 
          THEN pipeline_snapshot_base.opps
        ELSE 0
      END                                                                                                   AS won_deal_count,
 
      -----------------------------------------------------------------------------------
      -- NF: 20210201 DEPRECATED IACV fields
      
      CASE 
        WHEN pipeline_snapshot_base.is_open = 1 
          THEN pipeline_snapshot_base.incremental_acv
        ELSE 0                                                                                              
      END                                                                                                   AS open_iacv,

        CASE 
        WHEN pipeline_snapshot_base.stage_name_3plus IN ('3+ Pipeline')
          THEN pipeline_snapshot_base.incremental_acv
        ELSE 0
      END                                                                                                   AS open_3plus_iacv,
  
      CASE 
        WHEN pipeline_snapshot_base.stage_name_4plus IN ('4+ Pipeline')
          THEN pipeline_snapshot_base.incremental_acv
        ELSE 0
      END                                                                                                   AS open_4plus_iacv,

      CASE 
       WHEN pipeline_snapshot_base.is_won = 1 
          THEN pipeline_snapshot_base.incremental_acv
        ELSE 0  
      END                                                                                                   AS won_iacv,

      pipeline_snapshot_base.net_iacv                                                                       AS won_net_iacv,
      pipeline_snapshot_base.created_and_won_iacv,
      pipeline_snapshot_base.created_in_quarter_iacv,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields

      CASE 
        WHEN pipeline_snapshot_base.is_open = 1 
          THEN pipeline_snapshot_base.net_arr
        ELSE 0                                                                                              
      END                                                                                                   AS open_net_arr,

      CASE 
        WHEN pipeline_snapshot_base.stage_name_3plus IN ('3+ Pipeline')
          THEN pipeline_snapshot_base.net_arr
        ELSE 0
      END                                                                                                   AS open_3plus_net_arr,
  
      CASE 
        WHEN pipeline_snapshot_base.stage_name_4plus IN ('4+ Pipeline')
          THEN pipeline_snapshot_base.net_arr
        ELSE 0
      END                                                                                                   AS open_4plus_net_arr,

      CASE
        WHEN pipeline_snapshot_base.is_won = 1 
          OR (pipeline_snapshot_base.is_renewal = 1 AND pipeline_snapshot_base.is_lost = 1)
          THEN pipeline_snapshot_base.net_arr  
        ELSE 0
      END                                                                                                   AS won_net_arr,
      pipeline_snapshot_base.created_and_won_same_quarter_net_arr,
      pipeline_snapshot_base.created_in_snapshot_quarter_net_arr,

      -----------------------------------------------------------------------------------
      
      -- snapshot date fields
      pipeline_snapshot_base.snapshot_date,
      pipeline_snapshot_base.snapshot_fiscal_year,
      pipeline_snapshot_base.snapshot_fiscal_quarter_name,
      pipeline_snapshot_base.snapshot_fiscal_quarter_date,
      pipeline_snapshot_base.snapshot_day_of_fiscal_quarter

    FROM pipeline_snapshot_base
    -- till end of quarter
    WHERE       
      pipeline_snapshot_base.snapshot_date <= DATEADD(month,3,pipeline_snapshot_base.close_fiscal_quarter_date)
      -- 1 quarters before start
      AND pipeline_snapshot_base.snapshot_date >= DATEADD(month,-3,pipeline_snapshot_base.close_fiscal_quarter_date)
     
), reported_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT 
      pipeline_snapshot.close_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_fiscal_quarter_date,

      pipeline_snapshot.opportunity_owner_user_segment,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,

      pipeline_snapshot.snapshot_day_of_fiscal_quarter,
  
      pipeline_snapshot.opportunity_owner_rd_asm_level,
      pipeline_snapshot.sales_qualified_source,
      
      SUM(pipeline_snapshot.won_deal_count)                                           AS won_deal_count,
      SUM(pipeline_snapshot.open_deal_count)                                          AS open_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                                    AS open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                                    AS open_4plus_deal_count,

      -----------------------------------------------------------------------------------
      -- NF: 20210201 DEPRECATED IACV fields   
      -- open / won pipeline in quarter
      
      SUM(pipeline_snapshot.open_iacv)                                                AS open_iacv,
      SUM(pipeline_snapshot.open_3plus_iacv)                                          AS open_3plus_iacv,
      SUM(pipeline_snapshot.open_4plus_iacv)                                          AS open_4plus_iacv,
      SUM(pipeline_snapshot.won_iacv)                                                 AS won_iacv,
      SUM(pipeline_snapshot.won_net_iacv)                                             AS won_net_iacv,
      SUM(pipeline_snapshot.created_and_won_iacv)                                     AS created_and_won_iacv,

      -----------------------------------------------------------------------------------
      -- NF: 20210201  NET ARR fields
      
      SUM(pipeline_snapshot.open_net_arr)                                             AS open_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                                       AS open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                                       AS open_4plus_net_arr,
      SUM(pipeline_snapshot.won_net_arr)                                              AS won_net_arr,
      SUM(pipeline_snapshot.created_and_won_same_quarter_net_arr)                     AS created_and_won_same_quarter_net_arr

      -----------------------------------------------------------------------------------

    FROM pipeline_snapshot
    -- restrict the rows to pipeline of the quarter the snapshot was taken
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_name = pipeline_snapshot.close_fiscal_quarter_name
    -- to account for net iacv, it is needed to include lost renewal deals
    AND (LOWER(pipeline_snapshot.stage_name) NOT LIKE '%lost%' 
      OR (pipeline_snapshot.is_renewal = 1 AND LOWER(pipeline_snapshot.stage_name) LIKE '%lost%'))     
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  
), next_quarter AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter_name,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      pipeline_snapshot.close_fiscal_quarter_name                                  AS next_close_fiscal_quarter,
      pipeline_snapshot.close_fiscal_quarter_date                                  AS next_close_fiscal_quarter_date,
     
      pipeline_snapshot.opportunity_owner_user_segment,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.deal_group,

      pipeline_snapshot.snapshot_day_of_fiscal_quarter,
      pipeline_snapshot.opportunity_owner_rd_asm_level,
      pipeline_snapshot.sales_qualified_source,
      
      SUM(pipeline_snapshot.deal_count)                                        AS next_open_deal_count,
      SUM(pipeline_snapshot.open_3plus_deal_count)                             AS next_open_3plus_deal_count,
      SUM(pipeline_snapshot.open_4plus_deal_count)                             AS next_open_4plus_deal_count,

      ------------------------------
      -- DEPRECATED IACV METRICS

      SUM(pipeline_snapshot.incremental_acv)                                   AS next_open_iacv,
      SUM(pipeline_snapshot.open_3plus_iacv)                                   AS next_open_3plus_iacv,
      SUM(pipeline_snapshot.open_4plus_iacv)                                   AS next_open_4plus_iacv,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead

      SUM(pipeline_snapshot.open_net_arr)                                           AS next_open_net_arr,
      SUM(pipeline_snapshot.open_3plus_net_arr)                                     AS next_open_3plus_net_arr,
      SUM(pipeline_snapshot.open_4plus_net_arr)                                     AS next_open_4plus_net_arr

    FROM pipeline_snapshot
    -- restrict the report to show next quarter lines
    -- without this we would get results for multiple quarters
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -3,pipeline_snapshot.close_fiscal_quarter_date) 
      -- exclude lost deals from pipeline
      AND LOWER(pipeline_snapshot.stage_name) NOT LIKE '%lost%'   
    GROUP BY 1,2,3,4,5,6,7,8,9,10

), pipeline_gen AS (

    SELECT
      pipeline_snapshot_base.snapshot_fiscal_quarter_date,
      pipeline_snapshot_base.snapshot_day_of_fiscal_quarter,

      -------------------
      -- report keys
      pipeline_snapshot_base.opportunity_owner_user_segment,
      pipeline_snapshot_base.deal_category, 
      pipeline_snapshot_base.deal_group,
      pipeline_snapshot_base.opportunity_owner_rd_asm_level,
      pipeline_snapshot_base.sales_qualified_source,
      -------------------

      SUM(pipeline_snapshot_base.opps)                      AS created_in_quarter_count,

      ------------------------------
      -- DEPRECATED IACV METRICS
      SUM(pipeline_snapshot_base.incremental_acv)           AS created_in_quarter_iacv,

      ------------------------------
      -- Net ARR 
      -- Use Net ARR instead      
      SUM(pipeline_snapshot_base.net_arr)                   AS created_in_quarter_net_arr

    FROM pipeline_snapshot_base
    -- restrict the rows to pipeline of the quarter the snapshot was taken
    WHERE pipeline_snapshot_base.snapshot_fiscal_quarter_name = pipeline_snapshot_base.created_fiscal_quarter_name
      -- remove pre-opty deals
      -- remove pre-opty deals and stage 0
      AND pipeline_snapshot_base.stage_name NOT IN ('0-Pending Acceptance','10-Duplicate','00-Pre Opportunity','9-Unqualified')
    GROUP BY 1,2,3,4,5,6,7

), base_fields AS (
    
    SELECT DISTINCT 
      -----------------------------
      -- keys
      a.opportunity_owner_user_segment,
      e.opportunity_owner_rd_asm_level,
      b.deal_category,
      b.deal_group,
      -----------------------------
      f.sales_qualified_source,
      c.snapshot_fiscal_quarter_date,
      d.snapshot_fiscal_quarter_name,
      d.snapshot_day_of_fiscal_quarter,
      d.snapshot_next_fiscal_quarter_date
    FROM (SELECT DISTINCT opportunity_owner_user_segment FROM pipeline_snapshot_base) a
    CROSS JOIN (SELECT DISTINCT deal_category,
                                deal_group 
                FROM pipeline_snapshot_base) b
    CROSS JOIN (SELECT DISTINCT snapshot_fiscal_quarter_date FROM pipeline_snapshot_base) c
    CROSS JOIN (SELECT DISTINCT sales_qualified_source FROM pipeline_snapshot_base) f
    CROSS JOIN (SELECT DISTINCT opportunity_owner_rd_asm_level
                FROM pipeline_snapshot_base) e
    INNER JOIN (SELECT DISTINCT fiscal_quarter_name_fy                                                              AS snapshot_fiscal_quarter_name,
                              first_day_of_fiscal_quarter                                                           AS snapshot_fiscal_quarter_date, 
                              DATEADD(month,3,first_day_of_fiscal_quarter)                                          AS snapshot_next_fiscal_quarter_date,
                              day_of_fiscal_quarter_normalised                                                      AS snapshot_day_of_fiscal_quarter
              FROM date_details) d
      ON c.snapshot_fiscal_quarter_date = d.snapshot_fiscal_quarter_date 
), report_pipeline_metrics_day AS (
  
SELECT 
  -----------------------------
  -- keys
  base_fields.opportunity_owner_user_segment, 
  base_fields.opportunity_owner_rd_asm_level,
  base_fields.deal_category,
  base_fields.deal_group,
  base_fields.sales_qualified_source,
  -----------------------------

  LOWER(base_fields.deal_category) || '_' || LOWER(base_fields.opportunity_owner_user_segment)                      AS key_segment_report,
  LOWER(base_fields.sales_qualified_source) || '_' || LOWER(base_fields.opportunity_owner_user_segment)             AS key_sqs_report,
  
  base_fields.snapshot_fiscal_quarter_name                                                                          AS close_fiscal_quarter,
  base_fields.snapshot_fiscal_quarter_name,
  
  base_fields.snapshot_fiscal_quarter_date                                                                          AS close_fiscal_quarter_date,
  base_fields.snapshot_fiscal_quarter_date,
  
  base_fields.snapshot_day_of_fiscal_quarter,

  COALESCE(reported_quarter.open_deal_count,0)                AS open_pipeline_deal_count,
  COALESCE(reported_quarter.open_3plus_deal_count,0)          AS open_3plus_deal_count,
  COALESCE(reported_quarter.open_4plus_deal_count,0)          AS open_4plus_deal_count, 
  COALESCE(reported_quarter.won_deal_count,0)                 AS won_deal_count,
  pipeline_gen.created_in_quarter_count,

  COALESCE(next_quarter.next_open_deal_count,0)               AS next_open_deal_count,
  COALESCE(next_quarter.next_open_3plus_deal_count,0)         AS next_open_3plus_deal_count,
  COALESCE(next_quarter.next_open_4plus_deal_count,0)         AS next_open_4plus_deal_count,

  ------------------------------
  -- DEPRECATED IACV METRICS  

  COALESCE(reported_quarter.won_net_iacv,0)                   AS won_net_iacv,
  COALESCE(reported_quarter.won_iacv,0)                       AS won_iacv,
  COALESCE(reported_quarter.open_3plus_iacv,0)                AS open_3plus_pipeline_iacv, 
  COALESCE(reported_quarter.open_4plus_iacv,0)                AS open_4plus_pipeline_iacv, 
  COALESCE(reported_quarter.open_iacv,0)                      AS open_pipeline_iacv,

  reported_quarter.created_and_won_iacv,
  pipeline_gen.created_in_quarter_iacv,

  COALESCE(next_quarter.next_open_iacv,0)                     AS next_open_iacv,
  COALESCE(next_quarter.next_open_3plus_iacv,0)               AS next_open_3plus_iacv,
  COALESCE(next_quarter.next_open_4plus_iacv,0)               AS next_open_4plus_iacv,

  ------------------------------
  -- Net ARR 
  -- Use Net ARR instead     
  -- created and closed
  
  COALESCE(reported_quarter.won_net_arr,0)                    AS won_net_arr,
  COALESCE(reported_quarter.open_3plus_net_arr,0)             AS open_3plus_pipeline_net_arr, 
  COALESCE(reported_quarter.open_4plus_net_arr,0)             AS open_4plus_pipeline_net_arr, 
  COALESCE(reported_quarter.open_net_arr,0)                   AS open_pipeline_net_arr,
  
  reported_quarter.created_and_won_same_quarter_net_arr,
  pipeline_gen.created_in_quarter_net_arr,

  COALESCE(next_quarter.next_open_net_arr,0)                  AS next_open_net_arr,
  COALESCE(next_quarter.next_open_3plus_net_arr,0)            AS next_open_3plus_net_arr,
  COALESCE(next_quarter.next_open_4plus_net_arr,0)            AS next_open_4plus_net_arr,

  -- next quarter 
  next_quarter_date.fiscal_quarter_name_fy                    AS next_close_fiscal_quarter,
  next_quarter_date.first_day_of_fiscal_quarter               AS next_close_fiscal_quarter_date                   


-- created a list of all options to avoid having blanks when attaching totals in the reporting phase
FROM base_fields
-- historical quarter
LEFT JOIN reported_quarter
  ON base_fields.opportunity_owner_user_segment = reported_quarter.opportunity_owner_user_segment
  AND base_fields.snapshot_fiscal_quarter_date = reported_quarter.snapshot_fiscal_quarter_date
  AND base_fields.deal_category = reported_quarter.deal_category
  AND base_fields.snapshot_day_of_fiscal_quarter = reported_quarter.snapshot_day_of_fiscal_quarter
  AND base_fields.opportunity_owner_rd_asm_level = reported_quarter.opportunity_owner_rd_asm_level
  AND base_fields.sales_qualified_source = reported_quarter.sales_qualified_source
  AND base_fields.deal_group = reported_quarter.deal_group
-- next quarter in relation to the considered reported quarter
LEFT JOIN  next_quarter
  ON next_quarter.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
  AND next_quarter.opportunity_owner_user_segment = base_fields.opportunity_owner_user_segment
  AND next_quarter.deal_category = base_fields.deal_category
  AND next_quarter.snapshot_day_of_fiscal_quarter = base_fields.snapshot_day_of_fiscal_quarter
  AND next_quarter.opportunity_owner_rd_asm_level = base_fields.opportunity_owner_rd_asm_level
  AND next_quarter.sales_qualified_source = base_fields.sales_qualified_source
  AND next_quarter.deal_group = base_fields.deal_group
LEFT JOIN pipeline_gen 
  ON pipeline_gen.snapshot_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
  AND pipeline_gen.opportunity_owner_user_segment = base_fields.opportunity_owner_user_segment
  AND pipeline_gen.deal_category = base_fields.deal_category
  AND pipeline_gen.snapshot_day_of_fiscal_quarter = base_fields.snapshot_day_of_fiscal_quarter
  AND pipeline_gen.opportunity_owner_rd_asm_level = base_fields.opportunity_owner_rd_asm_level
  AND pipeline_gen.sales_qualified_source = base_fields.sales_qualified_source
  AND pipeline_gen.deal_group = base_fields.deal_group
LEFT JOIN date_details next_quarter_date
  ON next_quarter_date.date_actual = base_fields.snapshot_next_fiscal_quarter_date
)

SELECT *
FROM report_pipeline_metrics_day