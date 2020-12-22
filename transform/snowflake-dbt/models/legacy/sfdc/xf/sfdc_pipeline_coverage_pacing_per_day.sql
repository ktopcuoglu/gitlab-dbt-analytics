
 /*
TODO: 
2020-10-06: - Add external logic to track excluded. Maybe that logic can be added at the opportunity
            level and bring it in from the opportunity_report or xf object.
            - Refactor the flags added to the opportunity snapshot history, move them to that table instead
 */

WITH date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_opportunity_snapshot_history_xf AS (
    
    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history_xf') }}
    -- remove lost & deleted deals
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
      -- exclude deleted deals
      AND is_deleted = 0
      -- remove omitted deals
      AND forecast_category_name != 'Omitted'
      -- remove incomplete quarters, data from beggining of Q4 FY20
      AND snapshot_date >= '2019-11-01'::DATE
      -- remove excluded deals
      AND is_excluded_flag = 0

), pipeline_snapshot_base AS (
    
    SELECT
      snapshot_date,
      close_fiscal_quarter,
      close_fiscal_quarter_date,
      close_fiscal_year,
      order_type_stamped,

      -- sales team - region fields
      account_owner_team_stamped,
      account_owner_team_vp_level,
      account_owner_team_rd_level,
      account_owner_team_asm_level,
      account_owner_sales_region,
      
      stage_name_3plus,
      stage_name_4plus,
      is_excluded_flag,
      stage_name,
      forecast_category_name,

      adj_ultimate_parent_sales_segment,
      
      CASE 
        WHEN order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN order_type_stamped = '4. Churn'
          THEN '3. Churn'
        ELSE '4. Other'
      END                                                                         AS deal_category,

      -- the account hierarchy can be related to the VP / ASM / RD levels
      -- and to an approximate region
      account_owner_min_team_level,

      COUNT(DISTINCT opportunity_id)                                              AS opps,
      SUM(net_iacv)                                                               AS net_iacv,
      SUM(churn_only)                                                             AS churn_only,
      SUM(forecasted_iacv)                                                        AS forecasted_iacv,
      SUM(total_contract_value)                                                   AS tcv,

      SUM(created_and_won_iacv)                                                   AS created_and_won_iacv
    FROM sfdc_opportunity_snapshot_history_xf 
    WHERE 
      -- till end of quarter
      snapshot_date <= DATEADD(month,3,close_fiscal_quarter_date)
      -- 1 quarters before start
      AND snapshot_date >= DATEADD(month,-3,close_fiscal_quarter_date)
      {{ dbt_utils.group_by(n=18) }}

), pipeline_snapshot AS (

    SELECT 
      pipeline_snapshot_base.close_fiscal_quarter,
      pipeline_snapshot_base.close_fiscal_quarter_date,
      pipeline_snapshot_base.adj_ultimate_parent_sales_segment,
      
      -- sales team - region fields
      pipeline_snapshot_base.account_owner_min_team_level,
      pipeline_snapshot_base.account_owner_team_vp_level,
      pipeline_snapshot_base.account_owner_team_rd_level,
      pipeline_snapshot_base.account_owner_team_asm_level,
      pipeline_snapshot_base.account_owner_sales_region,

      pipeline_snapshot_base.deal_category,
      snapshot_date.day_of_fiscal_quarter,
      pipeline_snapshot_base.forecasted_iacv                                                                AS open_won_net_iacv,
      pipeline_snapshot_base.opps                                                                           AS open_won_deal_count,

      CASE 
        WHEN pipeline_snapshot_base.stage_name_3plus IN ('3+ Pipeline','Closed Won')
          THEN pipeline_snapshot_base.forecasted_iacv
        ELSE 0
      END                                                                                                   AS open_won_3plus_net_iacv,
      CASE 
        WHEN pipeline_snapshot_base.stage_name_3plus IN ('3+ Pipeline','Closed Won')
          THEN pipeline_snapshot_base.opps
        ELSE 0
      END                                                                                                   AS open_won_3plus_deal_count,
  
      CASE 
        WHEN LOWER(pipeline_snapshot_base.stage_name) LIKE '%won%'
          THEN pipeline_snapshot_base.net_iacv
        ELSE 0  
      END                                                                                                   AS won_net_iacv,
      CASE 
        WHEN LOWER(pipeline_snapshot_base.stage_name) LIKE '%won%'
          THEN pipeline_snapshot_base.opps
        ELSE 0
      END                                                                                                   AS won_deal_count,
  
      -- created and closed
      pipeline_snapshot_base.created_and_won_iacv,

      -- snapshot date fields
      pipeline_snapshot_base.snapshot_date,
      snapshot_date.fiscal_quarter_name_fy                                                                  AS snapshot_fiscal_quarter,
      snapshot_date.first_day_of_fiscal_quarter                                                             AS snapshot_fiscal_quarter_date,
      snapshot_date.day_of_fiscal_quarter                                                                   AS snapshot_day_of_fiscal_quarter
          
    FROM pipeline_snapshot_base
    -- Current day
    CROSS JOIN (SELECT *
                  FROM date_details
                  WHERE date_actual = DATEADD(day,-1,CURRENT_DATE)) today_date
    -- snapshot date
    INNER JOIN date_details snapshot_date
      ON snapshot_date.date_actual = pipeline_snapshot_base.snapshot_date
    -- exclude close lost
    WHERE LOWER(pipeline_snapshot_base.stage_name) NOT LIKE '%lost%'
      -- remove the 92 day
      AND snapshot_date.day_of_fiscal_quarter < 92
      -- exclude current quarter
      AND snapshot_date.fiscal_quarter_name_fy != today_date.fiscal_quarter_name_fy  

), previous_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT 
      pipeline_snapshot.close_fiscal_quarter,
      pipeline_snapshot.snapshot_fiscal_quarter,
      pipeline_snapshot.close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_fiscal_quarter_date,

      pipeline_snapshot.adj_ultimate_parent_sales_segment,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter,
  
      -- keys missing
      pipeline_snapshot.account_owner_min_team_level,
      -- channel
      
      -- open / won pipeline in quarter
      SUM(open_won_net_iacv)                                                          AS open_won_net_iacv,
      SUM(open_won_deal_count)                                                        AS open_won_deal_count,

      SUM(open_won_3plus_net_iacv)                                                    AS open_won_3plus_net_iacv,
      SUM(open_won_3plus_deal_count)                                                  AS open_won_3plus_deal_count,

      SUM(won_net_iacv)                                                               AS won_net_iacv,
      SUM(won_deal_count)                                                             AS won_deal_count,

      --created in quarter
      SUM(pipeline_snapshot.created_and_won_iacv)                                     AS created_and_won_iacv

    FROM pipeline_snapshot
    -- restrict the rows to pipeline of the quarter the snapshot was taken
    WHERE pipeline_snapshot.snapshot_fiscal_quarter = pipeline_snapshot.close_fiscal_quarter
    {{ dbt_utils.group_by(n=8) }}
  
), next_quarter AS (
    
    SELECT 
      pipeline_snapshot.snapshot_fiscal_quarter                                    AS close_fiscal_quarter,
      pipeline_snapshot.snapshot_fiscal_quarter_date                               AS close_fiscal_quarter_date,
      pipeline_snapshot.close_fiscal_quarter                                       AS next_close_fiscal_quarter,
      pipeline_snapshot.close_fiscal_quarter_date                                  AS next_close_fiscal_quarter_date,
      pipeline_snapshot.snapshot_fiscal_quarter,
      pipeline_snapshot.snapshot_fiscal_quarter_date,
      
      pipeline_snapshot.adj_ultimate_parent_sales_segment,
      pipeline_snapshot.deal_category,
      pipeline_snapshot.snapshot_day_of_fiscal_quarter,

      -- keys missing
      pipeline_snapshot.account_owner_min_team_level,
      -- channel

      SUM(open_won_net_iacv)                                                      AS next_open_net_iacv,
      SUM(open_won_deal_count)                                                    AS next_open_deal_count,

      SUM(open_won_3plus_net_iacv)                                                AS next_open_3plus_net_iacv,
      SUM(open_won_3plus_deal_count)                                              AS next_open_3plus_deal_count

    FROM pipeline_snapshot
    -- restrict the report to show next quarter lines
    -- without this we would get results for multiple quarters
    WHERE pipeline_snapshot.snapshot_fiscal_quarter_date = DATEADD(month, -3,pipeline_snapshot.close_fiscal_quarter_date)  
    {{ dbt_utils.group_by(n=10) }}

), base_fields AS (
    
    SELECT DISTINCT 
      a.adj_ultimate_parent_sales_segment,
      b.deal_category,
      e.account_owner_min_team_level,
      e.account_owner_sales_region,
      e.account_owner_team_vp_level,
      e.account_owner_team_rd_level,
      e.account_owner_team_asm_level,
      c.snapshot_fiscal_quarter_date,
      d.snapshot_fiscal_quarter,
      d.snapshot_day_of_fiscal_quarter,
      d.snapshot_next_fiscal_quarter_date
    FROM (SELECT DISTINCT adj_ultimate_parent_sales_segment FROM pipeline_snapshot) a
    CROSS JOIN (SELECT DISTINCT deal_category FROM pipeline_snapshot) b
    CROSS JOIN (SELECT DISTINCT snapshot_fiscal_quarter_date FROM pipeline_snapshot) c
    CROSS JOIN (SELECT DISTINCT account_owner_min_team_level,
                                account_owner_sales_region,
                                account_owner_team_vp_level,
                                account_owner_team_rd_level,
                                account_owner_team_asm_level
                FROM pipeline_snapshot) e
    INNER JOIN (SELECT DISTINCT fiscal_quarter_name_fy                                                              AS snapshot_fiscal_quarter,
                              first_day_of_fiscal_quarter                                                           AS snapshot_fiscal_quarter_date, 
                              DATEADD(month,3,first_day_of_fiscal_quarter)                                          AS snapshot_next_fiscal_quarter_date,
                              day_of_fiscal_quarter                                                                 AS snapshot_day_of_fiscal_quarter
              FROM date_details) d
      ON c.snapshot_fiscal_quarter_date = d.snapshot_fiscal_quarter_date 
)
  
SELECT 
  base_fields.adj_ultimate_parent_sales_segment                                                                     AS sales_segment, 
  base_fields.deal_category,
  base_fields.account_owner_min_team_level,
  base_fields.account_owner_team_vp_level,
  base_fields.account_owner_team_rd_level,
  base_fields.account_owner_team_asm_level,
  base_fields.account_owner_sales_region,
  LOWER(base_fields.deal_category) || '_' || LOWER(base_fields.adj_ultimate_parent_sales_segment)                   AS key_segment_report,
  LOWER(base_fields.account_owner_min_team_level) || '_' || LOWER(base_fields.adj_ultimate_parent_sales_segment)    AS key_region_report,
  base_fields.snapshot_fiscal_quarter                                                                               AS close_fiscal_quarter,
  base_fields.snapshot_fiscal_quarter,
  base_fields.snapshot_day_of_fiscal_quarter,
  COALESCE(previous_quarter.open_won_net_iacv,0) - COALESCE(previous_quarter.won_net_iacv,0)                        AS open_pipeline_net_iacv,
  COALESCE(previous_quarter.open_won_3plus_net_iacv,0)- COALESCE(previous_quarter.won_net_iacv,0)                   AS open_3plus_pipeline_net_iacv,  
  COALESCE(previous_quarter.won_net_iacv,0)                                                                         AS won_net_iacv,
  COALESCE(previous_quarter.open_won_deal_count,0) - COALESCE(previous_quarter.won_deal_count,0)                    AS open_pipeline_deal_count,
  COALESCE(previous_quarter.open_won_3plus_deal_count,0) - COALESCE(previous_quarter.won_deal_count,0)              AS open_3plus_deal_count,
  COALESCE(previous_quarter.won_deal_count,0)                                                                       AS won_deal_count,

  -- created and closed
  previous_quarter.created_and_won_iacv,
          
  -- next quarter 
  next_quarter_date.fiscal_quarter_name_fy                                                                          AS next_close_fiscal_quarter,
  next_quarter_date.first_day_of_fiscal_quarter                                                                     AS next_close_fiscal_quarter_date,                   
  next_quarter.next_open_net_iacv,
  next_quarter.next_open_3plus_net_iacv,
  next_quarter.next_open_deal_count,
  next_quarter.next_open_3plus_deal_count
       
FROM base_fields
LEFT JOIN previous_quarter
  ON base_fields.adj_ultimate_parent_sales_segment = previous_quarter.adj_ultimate_parent_sales_segment
  AND base_fields.snapshot_fiscal_quarter_date = previous_quarter.snapshot_fiscal_quarter_date
  AND base_fields.deal_category = previous_quarter.deal_category
  AND base_fields.snapshot_day_of_fiscal_quarter = previous_quarter.snapshot_day_of_fiscal_quarter
  AND base_fields.account_owner_min_team_level = previous_quarter.account_owner_min_team_level
LEFT JOIN  next_quarter
  ON next_quarter.close_fiscal_quarter_date = base_fields.snapshot_fiscal_quarter_date
  AND next_quarter.adj_ultimate_parent_sales_segment = base_fields.adj_ultimate_parent_sales_segment
  AND next_quarter.deal_category = base_fields.deal_category
  AND next_quarter.snapshot_day_of_fiscal_quarter = base_fields.snapshot_day_of_fiscal_quarter
  AND next_quarter.account_owner_min_team_level = base_fields.account_owner_min_team_level
LEFT JOIN date_details next_quarter_date
  ON next_quarter_date.date_actual = base_fields.snapshot_next_fiscal_quarter_date
