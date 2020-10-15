
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

), sfdc_pipeline_velocity_quarter AS (
    
    SELECT *
    FROM {{ ref('sfdc_pipeline_velocity_quarter') }}  

), sfdc_opportunity_xf AS (
    
    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}
    WHERE is_deleted = 0

), sfdc_opportunity_snapshot_history_xf AS (
    
    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history_xf') }}
    -- remove lost & deleted deals
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
        AND is_deleted = 0
        AND forecast_category_name != 'Omitted'
        -- remove incomplete quarters, data from beggining of Q4 FY20
        AND snapshot_date >= CAST('2019-11-01' AS DATE)

), pipeline_snapshot_base AS (
    
    SELECT  snapshot_date,
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
        
        CASE WHEN order_type_stamped = '1. New - First Order' 
                THEN '1. New'
            WHEN order_type_stamped IN ('2. New - Connected', '3. Growth') 
                THEN '2. Growth' 
            WHEN order_type_stamped = '4. Churn'
                THEN '3. Churn'
            ELSE '4. Other' END                                                     AS deal_category,

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
    snapshot_date <= dateadd(month,3,close_fiscal_quarter_date)
    -- 1 quarters before start
    AND snapshot_date >= dateadd(month,-3,close_fiscal_quarter_date)
    AND is_excluded_flag = 0

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

), pipeline_snapshot_extended AS (

    SELECT pq.close_fiscal_quarter,
            pq.close_fiscal_quarter_date,
            pq.adj_ultimate_parent_sales_segment,
            
            -- sales team - region fields
            pq.account_owner_min_team_level,
            pq.account_owner_team_vp_level,
            pq.account_owner_team_rd_level,
            pq.account_owner_team_asm_level,
            pq.account_owner_sales_region,

            pq.deal_category,
            dt.day_of_fiscal_quarter,
            pq.forecasted_iacv                                                               AS open_won_net_iacv,
            pq.opps                                                                          AS open_won_deal_count,
    
            CASE WHEN pq.stage_name_3plus in ('3+ Pipeline','Closed Won')
                    THEN pq.forecasted_iacv ELSE 0 END                                       AS open_won_3plus_net_iacv,
            CASE WHEN pq.stage_name_3plus in ('3+ Pipeline','Closed Won')
                    THEN pq.opps ELSE 0 END                                                  AS open_won_3plus_deal_count,
        
            CASE WHEN pq.stage_name like '%Won%'
                        THEN pq.net_iacv ELSE 0 END                                          AS won_net_iacv,
            CASE WHEN pq.stage_name like '%Won%'
                        THEN pq.opps ELSE 0 END                                              AS won_deal_count,
        
            -- created and closed
            pq.created_and_won_iacv,

            -- snapshot date fields
            pq.snapshot_date,
            dt.fiscal_quarter_name_fy                                                         AS snapshot_fiscal_quarter,
            dt.first_day_of_fiscal_quarter                                                    AS snapshot_fiscal_quarter_date,
            dt.day_of_fiscal_quarter                                                          AS snapshot_day_of_fiscal_quarter
                
   FROM pipeline_snapshot_base pq
   -- Current day
   CROSS JOIN (SELECT *
                FROM date_details
                WHERE date_actual = dateadd(day,-1,current_date)) da
   -- snapshot date
   INNER JOIN date_details dt
        ON dt.date_actual = pq.snapshot_date
  
   -- exclude close lost
   WHERE lower(pq.stage_name) not like '%lost%'
     -- and exclusion as per Pipeline Velocity file
     --AND is_excluded_flag = 0
     -- remove the 92 day
     AND dt.day_of_fiscal_quarter < 92
     -- exclude current quarter
     AND dt.fiscal_quarter_name_fy != da.fiscal_quarter_name_fy  

), previous_quarter AS (
  
    -- daily snapshot of pipeline metrics per quarter within the quarter
    SELECT pq.close_fiscal_quarter,
        pq.snapshot_fiscal_quarter,
        pq.close_fiscal_quarter_date,
        pq.snapshot_fiscal_quarter_date,

        pq.adj_ultimate_parent_sales_segment,
        pq.deal_category,
        pq.snapshot_day_of_fiscal_quarter,
   
        -- keys missing
        pq.account_owner_min_team_level,
        -- channel
        
        -- open / won pipeline in quarter
        sum(open_won_net_iacv)                                                          AS open_won_net_iacv,
        sum(open_won_deal_count)                                                        AS open_won_deal_count,

        sum(open_won_3plus_net_iacv)                                                    AS open_won_3plus_net_iacv,
        sum(open_won_3plus_deal_count)                                                  AS open_won_3plus_deal_count,

        sum(won_net_iacv)                                                               AS won_net_iacv,
        sum(won_deal_count)                                                             AS won_deal_count,

        --created in quarter
        sum(pq.created_and_won_iacv)                                                    AS created_and_won_iacv

   FROM pipeline_snapshot_extended pq
   -- restrict the rows to pipeline of the quarter the snapshot was taken
   WHERE pq.snapshot_fiscal_quarter = pq.close_fiscal_quarter

   GROUP BY pq.close_fiscal_quarter,
            pq.snapshot_fiscal_quarter,
            pq.snapshot_fiscal_quarter_date,
            pq.close_fiscal_quarter_date,
            pq.adj_ultimate_parent_sales_segment,
            pq.deal_category,
            pq.account_owner_min_team_level,
            pq.snapshot_day_of_fiscal_quarter
 
), next_quarter AS (
    
        SELECT pq.snapshot_fiscal_quarter                                              AS close_fiscal_quarter,
          pq.snapshot_fiscal_quarter_date                                              AS close_fiscal_quarter_date,
          pq.close_fiscal_quarter                                                      AS next_close_fiscal_quarter,
          pq.close_fiscal_quarter_date                                                 AS next_close_fiscal_quarter_date,
          pq.snapshot_fiscal_quarter,
          pq.snapshot_fiscal_quarter_date,
          
          pq.adj_ultimate_parent_sales_segment,
          pq.deal_category,
          pq.snapshot_day_of_fiscal_quarter,
   
          -- keys missing
          pq.account_owner_min_team_level,
          -- channel
   
          sum(open_won_net_iacv)                                                      AS next_open_net_iacv,
          sum(open_won_deal_count)                                                    AS next_open_deal_count,

          sum(open_won_3plus_net_iacv)                                                AS next_open_3plus_net_iacv,
          sum(open_won_3plus_deal_count)                                              AS next_open_3plus_deal_count

   FROM pipeline_snapshot_extended pq
   -- restrict the report to show next quarter lines
   -- without this we would get results for multiple quarters
   WHERE pq.snapshot_fiscal_quarter_date = dateadd(month, -3,pq.close_fiscal_quarter_date)  
   GROUP BY pq.close_fiscal_quarter,
            pq.close_fiscal_quarter_date,
            pq.snapshot_fiscal_quarter,
            pq.snapshot_fiscal_quarter_date,      
            pq.adj_ultimate_parent_sales_segment,
            pq.deal_category,
            pq.account_owner_min_team_level,
            pq.snapshot_day_of_fiscal_quarter

), data_structure AS (
    
    SELECT DISTINCT a.adj_ultimate_parent_sales_segment,
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
         FROM (SELECT DISTINCT adj_ultimate_parent_sales_segment FROM pipeline_snapshot_extended) a
            CROSS JOIN (SELECT DISTINCT deal_category FROM pipeline_snapshot_extended) b
            CROSS JOIN (SELECT DISTINCT snapshot_fiscal_quarter_date FROM pipeline_snapshot_extended) c
            CROSS JOIN (SELECT DISTINCT account_owner_min_team_level,
                                        account_owner_sales_region,
                                        account_owner_team_vp_level,
                                        account_owner_team_rd_level,
                                        account_owner_team_asm_level
                                        FROM pipeline_snapshot_extended) e
            INNER JOIN (SELECT DISTINCT fiscal_quarter_name_fy                                               AS snapshot_fiscal_quarter,
                                        first_day_of_fiscal_quarter                                          AS snapshot_fiscal_quarter_date, 
                                        dateadd(month,3,first_day_of_fiscal_quarter)                         AS snapshot_next_fiscal_quarter_date,
                                        day_of_fiscal_quarter                                                AS snapshot_day_of_fiscal_quarter
                        FROM date_details) d
                ON c.snapshot_fiscal_quarter_date = d.snapshot_fiscal_quarter_date 
)
  
SELECT de.adj_ultimate_parent_sales_segment                                                                   AS sales_segment, 
        de.deal_category,
        de.account_owner_min_team_level,
        de.account_owner_team_vp_level,
        de.account_owner_team_rd_level,
        de.account_owner_team_asm_level,
        de.account_owner_sales_region,
        lower(de.deal_category) || '_' || lower(de.adj_ultimate_parent_sales_segment)                         AS key_segment_report,
        lower(de.account_owner_min_team_level) || '_' || lower(de.adj_ultimate_parent_sales_segment)      AS key_region_report,
        de.snapshot_fiscal_quarter                                                                            AS close_fiscal_quarter,
        de.snapshot_fiscal_quarter,
        de.snapshot_day_of_fiscal_quarter,
        coalesce(pq.open_won_net_iacv,0) - coalesce(pq.won_net_iacv,0)                                        AS open_pipeline_net_iacv,
        coalesce(pq.open_won_3plus_net_iacv,0)- coalesce(pq.won_net_iacv,0)                                   AS open_3plus_pipeline_net_iacv,  
        coalesce(pq.won_net_iacv,0)                                                                           AS won_net_iacv,
        coalesce(pq.open_won_deal_count,0) - coalesce(pq.won_deal_count,0)                                    AS open_pipeline_deal_count,
        coalesce(pq.open_won_3plus_deal_count,0) - coalesce(pq.won_deal_count,0)                              AS open_3plus_deal_count,
        coalesce(pq.won_deal_count,0)                                                                         AS won_deal_count,

        -- created and closed
        pq.created_and_won_iacv,
                
        -- next quarter 
        nqd.fiscal_quarter_name_fy                                                                           AS next_close_fiscal_quarter,
        nqd.first_day_of_fiscal_quarter                                                                      AS next_close_fiscal_quarter_date,                   
        nq.next_open_net_iacv,
        nq.next_open_3plus_net_iacv,
        nq.next_open_deal_count,
        nq.next_open_3plus_deal_count
       
 FROM data_structure de
    LEFT JOIN previous_quarter pq
        ON de.adj_ultimate_parent_sales_segment = pq.adj_ultimate_parent_sales_segment
        AND de.snapshot_fiscal_quarter_date = pq.snapshot_fiscal_quarter_date
        AND de.deal_category = pq.deal_category
        AND de.snapshot_day_of_fiscal_quarter = pq.snapshot_day_of_fiscal_quarter
        AND de.account_owner_min_team_level = pq.account_owner_min_team_level
    LEFT JOIN  next_quarter nq
        ON nq.close_fiscal_quarter_date = de.snapshot_fiscal_quarter_date
            AND nq.adj_ultimate_parent_sales_segment = de.adj_ultimate_parent_sales_segment
            AND nq.deal_category = de.deal_category
            AND nq.snapshot_day_of_fiscal_quarter = de.snapshot_day_of_fiscal_quarter
            AND nq.account_owner_min_team_level = de.account_owner_min_team_level
    LEFT JOIN date_details nqd
        ON nqd.date_actual = de.snapshot_next_fiscal_quarter_date
