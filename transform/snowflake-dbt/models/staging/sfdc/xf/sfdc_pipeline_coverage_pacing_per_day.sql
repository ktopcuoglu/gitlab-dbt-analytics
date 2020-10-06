
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
), pipeline_snapshot_base AS (
SELECT  snapshot_date,
        close_fiscal_quarter,
        close_fiscal_quarter_date,
        close_fiscal_year,
        order_type_stamped,
        account_owner_team_stamped,
        sales_segment,
        stage_name_3plus,
        stage_name_4plus,
        is_excluded_flag,
        stage_name,
        forecast_category_name,

        CASE WHEN sales_segment = 'Unknown' 
            THEN 'SMB' ELSE sales_segment END                                       AS adj_sales_segment,
        
        CASE WHEN order_type_stamped = '1. New - First Order' 
                THEN '1. New'
            WHEN order_type_stamped IN ('2. New - Connected', '3. Growth') 
                THEN '2. Growth' 
            WHEN order_type_stamped = '4. Churn'
                THEN '3. Churn'
            ELSE '4. Other' END                                                     AS deal_category,

        CASE WHEN account_owner_team_stamped in ('APAC', 'MM - APAC')
            THEN 'APAC'
        WHEN account_owner_team_stamped in ('MM - EMEA', 'EMEA', 'MM-EMEA')
            THEN 'EMEA'
        WHEN account_owner_team_stamped in ('US East', 'MM - East')
            THEN 'US EAST'
        WHEN account_owner_team_stamped in ('US West', 'MM - West')
            THEN 'US WEST'
        WHEN account_owner_team_stamped in ('Public Sector')
            THEN 'PUBSEC'
        ELSE 'OTHER' END                                                            AS adj_region,

        COUNT(DISTINCT opportunity_id)                          AS opps,
        SUM(net_iacv)                                           AS net_iacv,
        SUM(churn_only)                                         AS churn_only,
        SUM(forecasted_iacv)                                    AS forecasted_iacv,
        SUM(total_contract_value)                               AS tcv,

        SUM(created_and_won_iacv)                               AS created_and_won_iacv
FROM sfdc_opportunity_snapshot_history_xf 
WHERE 
    -- till end of quarter
    snapshot_date <= dateadd(month,3,close_fiscal_quarter_date)
    -- 1 quarters before start
    AND snapshot_date >= dateadd(month,-3,close_fiscal_quarter_date)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
), opty_base AS (
   SELECT o.*,
        
        CASE WHEN order_type_stamped = '1. New - First Order' 
                THEN '1. New'
            WHEN order_type_stamped IN ('2. New - Connected', '3. Growth') 
                THEN '2. Growth' 
            WHEN order_type_stamped = '4. Churn'
                THEN '3. Churn'
            ELSE '4. Other' END                                                             AS deal_category,

         CASE WHEN o.ultimate_parent_sales_segment IS NULL 
                OR o.ultimate_parent_sales_segment = 'Unknown' THEN 'SMB'
                ELSE o.ultimate_parent_sales_segment END                                    AS adj_sales_segment,
                
         CASE WHEN o.account_owner_team_stamped in ('APAC', 'MM - APAC')
                THEN 'APAC'
            WHEN o.account_owner_team_stamped in ('MM - EMEA', 'EMEA', 'MM-EMEA')
                THEN 'EMEA'
            WHEN o.account_owner_team_stamped in ('US East', 'MM - East')
                THEN 'US EAST'
            WHEN o.account_owner_team_stamped in ('US West', 'MM - West')
                THEN 'US WEST'
            WHEN o.account_owner_team_stamped in ('Public Sector')
                THEN 'PUBSEC'
            ELSE 'OTHER' END                                                                AS adj_region,

        -- date fields
        d.fiscal_quarter_name_fy                                                            AS close_fiscal_quarter_name,
        dc.fiscal_quarter_name_fy                                                           AS created_fiscal_quarter_name
   FROM sfdc_opportunity_xf o
    -- close date
    INNER JOIN date_details d
        ON d.date_actual = cast(o.close_date as date)
    --created date
    INNER JOIN date_details dc
        ON dc.date_actual = cast(o.created_date as date)
), pipeline_snapshot_extended AS (
    SELECT pq.close_fiscal_quarter,
            pq.close_fiscal_quarter_date,
            pq.adj_sales_segment,
            pq.adj_region,
            pq.deal_category,
            dt.day_of_fiscal_quarter,
            pq.forecasted_iacv                                                                AS open_won_net_iacv,
            pq.opps                                                                           AS open_won_deal_count,
    
            CASE WHEN pq.stage_name_3plus in ('3+ Pipeline','Closed Won')
                    THEN pq.forecasted_iacv ELSE 0 END                                        AS open_won_3plus_net_iacv,
            CASE WHEN pq.stage_name_3plus in ('3+ Pipeline','Closed Won')
                    THEN pq.opps ELSE 0 END                                                   AS open_won_3plus_deal_count,
        
            CASE WHEN pq.stage_name like '%Won%'
                        THEN pq.net_iacv ELSE 0 END                                           AS won_net_iacv,
            CASE WHEN pq.stage_name like '%Won%'
                        THEN pq.opps ELSE 0 END                                               AS won_deal_count,
        
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
     AND is_excluded_flag = 0
     -- remove the 92 day
     AND dt.day_of_fiscal_quarter < 92
     -- exclude current quarter
     AND dt.fiscal_quarter_name_fy != da.fiscal_quarter_name_fy
   
 ), total_per_quarter AS (
 -- gather the total value / count per quarter for previous quarters
 SELECT dc.first_day_of_fiscal_quarter                                          AS close_fiscal_quarter_date,
      dc.fiscal_quarter_name_fy                                                 AS close_fiscal_quarter,
      o.deal_category,
      o.adj_sales_segment                                                       AS sales_segment,
      o.adj_region,

      sum(o.net_incremental_acv)                                                AS total_won_net_iacv,
      sum(o.incremental_acv)                                                    AS total_won_iacv,
      count(o.opportunity_id)                                                   AS total_won_deal_count,

      sum(CASE WHEN o.created_fiscal_quarter_name = o.close_fiscal_quarter_name
            AND lower(o.stage_name) like '%won%' 
                THEN o.net_incremental_acv
                ELSE 0 END)                                                     AS total_created_and_won_net_iacv,

      sum(CASE WHEN o.created_fiscal_quarter_name = o.close_fiscal_quarter_name
            AND lower(o.stage_name) like '%won%' 
                THEN o.incremental_acv
                ELSE 0 END)                                                     AS total_created_and_won_iacv
      
 FROM opty_base o
 INNER JOIN date_details dc
    ON dc.date_actual = o.close_date
 WHERE o.stage_name like '%Won%'
   AND dc.fiscal_year >=2020
 GROUP BY dc.first_day_of_fiscal_quarter
    , dc.fiscal_quarter_name_fy 
    , o.adj_sales_segment
    , o.adj_region
    , o.deal_category
 ), previous_quarter AS (
  -- daily snapshot of pipeline metrics per quarter within the quarter
  SELECT pq.close_fiscal_quarter,
        pq.snapshot_fiscal_quarter,
        pq.close_fiscal_quarter_date,
        pq.adj_sales_segment,
        pq.deal_category,
        pq.snapshot_day_of_fiscal_quarter,
   
        -- keys missing
        pq.adj_region,
        -- channel
        
        -- open / won pipeline in quarter
        sum(open_won_net_iacv)                                                      AS open_won_net_iacv,
        sum(open_won_deal_count)                                                    AS open_won_deal_count,

        sum(open_won_3plus_net_iacv)                                                AS open_won_3plus_net_iacv,
        sum(open_won_3plus_deal_count)                                              AS open_won_3plus_deal_count,

        sum(won_net_iacv)                                                           AS won_net_iacv,
        sum(won_deal_count)                                                         AS won_deal_count,

        --created in quarter
        sum(pq.created_and_won_iacv)                                                AS created_and_won_iacv

   FROM pipeline_snapshot_extended pq
   -- restrict the rows to pipeline of the quarter the snapshot was taken
   WHERE pq.snapshot_fiscal_quarter = pq.close_fiscal_quarter

   GROUP BY pq.close_fiscal_quarter,
            pq.snapshot_fiscal_quarter,
            pq.close_fiscal_quarter_date,
            pq.adj_sales_segment,
            pq.deal_category,
            pq.adj_region,
            pq.snapshot_day_of_fiscal_quarter
 
 ), next_quarter AS (
    SELECT pq.snapshot_fiscal_quarter                                                  AS close_fiscal_quarter,
          pq.snapshot_fiscal_quarter_date                                              AS close_fiscal_quarter_date,
          pq.close_fiscal_quarter                                                      AS next_close_fiscal_quarter,
          pq.close_fiscal_quarter_date                                                 AS next_close_fiscal_quarter_date,
          pq.snapshot_fiscal_quarter,
          pq.snapshot_fiscal_quarter_date,
          
          pq.adj_sales_segment,
          pq.deal_category,
          pq.snapshot_day_of_fiscal_quarter,
   
          -- keys missing
          pq.adj_region,
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
            pq.adj_sales_segment,
            pq.deal_category,
            pq.adj_region,
            pq.snapshot_day_of_fiscal_quarter
  ) 
  SELECT pq.adj_sales_segment                                                           AS sales_segment, 
        pq.adj_region                                                                   AS region,
        lower(pq.deal_category) || '_' || lower(pq.adj_sales_segment)                   AS key_segment_report,
        lower(pq.adj_region) || '_' || lower(pq.adj_sales_segment)                      AS key_region_report,
        pq.close_fiscal_quarter,
        pq.snapshot_fiscal_quarter,
        pq.snapshot_day_of_fiscal_quarter,
        coalesce(pq.open_won_net_iacv,0) - coalesce(pq.won_net_iacv,0)                  AS open_pipeline_net_iacv,
        coalesce(pq.open_won_3plus_net_iacv,0)- coalesce(pq.won_net_iacv,0)             AS open_3plus_pipeline_net_iacv,  
        coalesce(pq.won_net_iacv,0)                                                     AS won_net_iacv,
        tq.total_won_net_iacv,
        coalesce(pq.open_won_deal_count,0) - coalesce(pq.won_deal_count,0)              AS open_pipeline_deal_count,
        coalesce(pq.open_won_3plus_deal_count,0) - coalesce(pq.won_deal_count,0)        AS open_3plus_deal_count,
        coalesce(pq.won_deal_count,0)                                                   AS won_deal_count,
        tq.total_won_deal_count,

         -- created and closed
        pq.created_and_won_iacv,
        tq.total_created_and_won_net_iacv,
          
        -- next quarter 
        nq.next_close_fiscal_quarter,
        nq.next_open_net_iacv,
        nq.next_open_3plus_net_iacv,
        nq.next_open_deal_count,
        nq.next_open_3plus_deal_count,
          
        -- next quarter totals 
        ntq.total_won_deal_count                                                            AS next_total_won_deal_count,
        ntq.total_won_net_iacv                                                              AS next_total_won_net_iacv   
          
       /* -- caculated ratios
        -- open coverage
        open_pipeline_net_iacv / (tq.total_won_net_iacv - won_net_iacv)                     AS open_coverage_day,
        open_3plus_pipeline_net_iacv / (tq.total_won_net_iacv - won_net_iacv)               AS open_3plus_coverage_day,

        won_net_iacv / tq.total_won_net_iacv                                                AS pacing_won_qtd_iacv,
        won_deal_count / tq.total_won_deal_count                                            AS pacing_won_deal_count,

        nq.next_open_net_iacv /  ntq.total_won_net_iacv                                     AS next_open_coverage,
        nq.next_open_3plus_net_iacv /  ntq.total_won_net_iacv                               AS next_open_3plus_coverage
*/
 FROM previous_quarter pq
    -- total for the "previous quarter" perspective
    INNER JOIN total_per_quarter tq
        ON tq.close_fiscal_quarter_date = pq.close_fiscal_quarter_date
            AND tq.sales_segment = pq.adj_sales_segment
            AND tq.deal_category = pq.deal_category
            AND tq.adj_region = pq.adj_region
    INNER JOIN next_quarter nq
        ON nq.close_fiscal_quarter_date = pq.close_fiscal_quarter_date
            AND nq.adj_sales_segment = pq.adj_sales_segment
            AND nq.deal_category = pq.deal_category
            AND nq.snapshot_day_of_fiscal_quarter = pq.snapshot_day_of_fiscal_quarter
            AND nq.adj_region = pq.adj_region
    -- total of the "next quarter" perspective
    INNER JOIN total_per_quarter ntq
        ON ntq.close_fiscal_quarter = nq.next_close_fiscal_quarter
            AND ntq.sales_segment = nq.adj_sales_segment
            AND ntq.deal_category = nq.deal_category
            AND ntq.adj_region = nq.adj_region
WHERE pq.adj_sales_segment is not null
