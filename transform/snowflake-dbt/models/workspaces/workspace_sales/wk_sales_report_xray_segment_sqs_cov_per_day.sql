WITH target_date AS (
    -- used to filter the amount of rows to download into the Pipeline X-Ray file
    SELECT 
      90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS day_of_fiscal_quarter_normalised,
      12-floor((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)/7))  AS week_of_fiscal_quarter_normalised,
      first_day_of_fiscal_quarter                                           AS current_fiscal_quarter_date
    FROM  {{ref('date_details')}} 
    WHERE date_actual BETWEEN dateadd(day,-1,CURRENT_DATE) and dateadd(day,7,CURRENT_DATE)
)
, report_pipeline_metrics_day AS (
  
    SELECT snaps.*
    FROM  {{ref('wk_sales_report_pipeline_metrics_per_day')}} snaps
    -- used to constraint the extract to the few rows that correspond to the quarter day
    INNER JOIN target_date
      ON target_date.day_of_fiscal_quarter_normalised = snaps.snapshot_day_of_fiscal_quarter_normalised
    WHERE snaps.snapshot_fiscal_quarter_date >= dateadd(month,-9,target_date.current_fiscal_quarter_date) 
)
-----------------------------------------------------
-----------------------------------------------------
, sfdc_opportunity_xf AS (
  
  SELECT *
  FROM  {{ref('wk_sales_sfdc_opportunity_xf')}} 
  
)
, detail_segment AS (
  SELECT 
        --------------------------
        -- Keys
         sales_team_cro_level,
         coalesce(sales_qualified_source,'Other') AS sales_qualified_source,
        
         --------------------------
  
         close_fiscal_quarter_name,
         close_fiscal_quarter_date,
         next_close_fiscal_quarter_name,
         snapshot_fiscal_quarter_name,
         snapshot_fiscal_quarter_date,
         snapshot_day_of_fiscal_quarter_normalised,
         
         ----------------------------------------------------------------------
         ----------------------------------------------------------------------

         SUM(won_deal_count)                      AS won_deal_count,
         
         SUM(open_1plus_deal_count)               AS open_1plus_deal_count,
         SUM(open_3plus_deal_count)               AS open_3plus_deal_count,
         SUM(open_4plus_deal_count)               AS open_4plus_deal_count,
   
         SUM(next_open_1plus_deal_count)          AS next_open_1plus_deal_count,
         SUM(next_open_3plus_deal_count)          AS next_open_3plus_deal_count,
         SUM(created_in_quarter_count)            AS created_in_quarter_count,
  
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
  
         SUM(won_net_iacv)                        AS won_net_iacv,
         SUM(open_1plus_iacv)                     AS open_pipeline_iacv,
         SUM(open_3plus_pipeline_iacv)            AS open_3plus_pipeline_iacv,  
         SUM(open_4plus_pipeline_iacv)            AS open_4plus_pipeline_iacv,
    
         -- next quarter

         SUM(next_open_iacv)                      AS next_open_iacv,
         SUM(next_open_3plus_iacv)                AS next_open_3plus_iacv,
         SUM(created_and_won_iacv)                AS created_and_won_iacv,
         SUM(created_in_quarter_iacv)             AS created_in_quarter_iacv,
  
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
  
         SUM(won_net_arr)                         AS won_net_arr,
         SUM(open_1plus_net_arr)                  AS open_1plus_net_arr,
         SUM(open_3plus_net_arr)                  AS open_3plus_net_arr,  
         SUM(open_4plus_net_arr)                  AS open_4plus_net_arr,
    
         -- next quarter
         SUM(next_open_1plus_net_arr)             AS next_open_1plus_net_arr,
         SUM(next_open_3plus_net_arr)             AS next_open_3plus_net_arr,
  
         SUM(created_and_won_same_quarter_net_arr)    AS created_and_won_same_quarter_net_arr,
         SUM(created_in_quarter_net_arr)              AS created_in_quarter_net_arr
  
 FROM report_pipeline_metrics_day 
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
, total_per_quarter AS (
  -- gather the total value / count per quarter for previous quarters
 SELECT 
        --------------------------
        -- Keys
        o.sales_team_cro_level,
        coalesce(o.sales_qualified_source,'Other') AS sales_qualified_source,
        --------------------------
  
        o.close_fiscal_quarter_date,
        o.close_fiscal_quarter_name,
     
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- deal count
        SUM(CASE 
              WHEN o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0
           END)                                                                     AS total_won_deal_count,
  
        SUM(CASE 
              WHEN o.pipeline_created_fiscal_quarter_name = o.close_fiscal_quarter_name
                THEN o.calculated_deal_count
              ELSE 0
           END)                                                                     AS total_created_deal_count,
  
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- NET ARR

        SUM(CASE
              WHEN (o.is_won = 1 OR (o.is_renewal = 1 and o.is_lost = 1))
                THEN o.net_arr
              ELSE 0 
            END)                                                                    AS total_won_net_arr,


        SUM(CASE 
              WHEN o.pipeline_created_fiscal_quarter_name = o.close_fiscal_quarter_name
                AND (o.is_won = 1 OR (o.is_renewal = 1 and o.is_lost = 1))
                  THEN o.net_arr
              ELSE 0 
            END)                                                                    AS total_created_and_won_net_arr,
      
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- IACV
  
        SUM(CASE 
              WHEN (o.is_won = 1 OR (o.is_renewal = 1 and o.is_lost = 1))
                  THEN o.net_incremental_acv
              ELSE 0 
            END)                                                                    AS total_won_net_iacv,
  
        SUM(CASE
              WHEN o.is_won = 1
                THEN o.incremental_acv
              ELSE 0 
            END)                                                                    AS total_won_iacv,

        SUM(CASE 
              WHEN o.pipeline_created_fiscal_quarter_name = o.close_fiscal_quarter_name
                AND (o.is_won = 1 OR (o.is_renewal = 1 and o.is_lost = 1))
                  THEN o.net_incremental_acv
              ELSE 0 
            END)                                                                    AS total_created_and_won_net_iacv,

        SUM(CASE 
              WHEN o.pipeline_created_fiscal_quarter_name = o.close_fiscal_quarter_name
                AND o.is_won = 1 
                  THEN o.incremental_acv
              ELSE 0 
            END)                                                                    AS total_created_and_won_iacv
       
 FROM sfdc_opportunity_xf o
 -- current day
 CROSS JOIN (SELECT *
             FROM legacy.date_details
             WHERE date_actual = DATEADD(day,-1,CURRENT_DATE)) today_date 
 -- won deals or churned renewals so we account all Net IACV
 WHERE (o.is_won = 1 OR (o.is_renewal = 1 AND o.is_lost = 1))
   AND o.close_fiscal_year >= 2020
   AND o.is_excluded_flag = 0
   AND o.is_deleted = 0
   AND today_date.first_day_of_fiscal_quarter > o.close_fiscal_quarter_date
 GROUP BY 1, 2, 3, 4
)
SELECT DISTINCT 
        --------------------------
        -- ORDER IS CRITICAL FOR REPORT TO WORK
        --------------------------
        -- Keys
        s.sales_team_cro_level,
        coalesce(s.sales_qualified_source,'Other') AS sales_qualified_source,
        --------------------------
        
        s.close_fiscal_quarter_name,
        s.snapshot_fiscal_quarter_name,
        s.snapshot_day_of_fiscal_quarter_normalised,
        s.next_close_fiscal_quarter_name,

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- Count of deals

        -- current quarter at snapshot day 
        s.won_deal_count,
        s.open_1plus_deal_count,
        s.open_3plus_deal_count,
        s.open_4plus_deal_count,
        qt.total_won_deal_count, -- total at quarter end

        s.created_in_quarter_count,
        qt.total_created_deal_count,

        -- next quarter
        s.next_open_1plus_deal_count,
        s.next_open_3plus_deal_count,
        qn.total_won_deal_count                                        AS next_total_won_deal_count, -- total at quarter end

        -- pacing & coverage
        CASE
          WHEN  qt.total_won_net_iacv  <> 0
            THEN s.won_deal_count / qt.total_won_deal_count
          ELSE Null 
         END                                                           AS pacing_won_deal_count,

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- IACV Deprecated fields
        s.won_net_iacv,
        s.open_pipeline_iacv,
        s.open_3plus_pipeline_iacv,  
        s.open_4plus_pipeline_iacv,    
        qt.total_won_net_iacv,
        
        -- next quarter
         s.next_open_iacv,
         s.next_open_3plus_iacv,

         -- next quarter 
         qn.total_won_net_iacv                                                              AS next_total_won_iacv,

         -- open coverage
         CASE 
           WHEN (qt.total_won_net_iacv - s.won_net_iacv)  > 0 
             THEN s.open_pipeline_iacv / (qt.total_won_net_iacv - s.won_net_iacv)                 
           ELSE NULL 
         END                                                                                 AS open_1plus_coverage_iacv,
         CASE 
           WHEN (qt.total_won_net_iacv - s.won_net_iacv)  > 0 
             THEN s.open_3plus_pipeline_iacv / (qt.total_won_net_iacv - s.won_net_iacv) 
           ELSE NULL 
         END                                                                                 AS open_3plus_coverage_iacv,
         CASE 
           WHEN (qt.total_won_net_iacv - s.won_net_iacv)  > 0 
             THEN s.open_4plus_pipeline_iacv / (qt.total_won_net_iacv - s.won_net_iacv) 
           ELSE NULL 
         END                                                                                 AS open_4plus_coverage_iacv,

         CASE 
           WHEN  qt.total_won_net_iacv  <> 0
             THEN s.won_net_iacv / qt.total_won_net_iacv
           ELSE Null 
         END                                                                                 AS pacing_won_qtd_iacv,

         CASE 
           WHEN qn.total_won_net_iacv  <> 0
             THEN s.next_open_iacv /  qn.total_won_net_iacv 
           ELSE Null 
         END                                                                                 AS next_open_1plus_coverage_iacv,

         CASE 
           WHEN qn.total_won_net_iacv  <> 0
             THEN s.next_open_3plus_iacv /  qn.total_won_net_iacv
           ELSE Null
         END                                                                                 AS next_open_3plus_coverage_iacv,

         s.created_and_won_iacv,
         qt.total_created_and_won_net_iacv,
         
         -- generated pipeline in quarter
         -- 20201206 - NF - Need to adjust for Vision ops messing historical perspective
         CASE 
          WHEN s.close_fiscal_quarter_name = 'FY21-Q2'
             AND s.snapshot_day_of_fiscal_quarter_normalised BETWEEN 45 AND 85
            THEN null 
          ELSE s.created_in_quarter_iacv 
         END                                                                                 AS created_in_quarter_iacv,

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- Net ARR
        s.won_net_arr,
        s.open_1plus_net_arr,
        s.open_3plus_net_arr,  
        s.open_4plus_net_arr,    
        qt.total_won_net_arr,
        
        -- next quarter
        s.next_open_1plus_net_arr,
        s.next_open_3plus_net_arr,

        -- next quarter 
        qn.total_won_net_arr                                                                AS next_total_won_net_arr,

        -- open coverage
        CASE 
          WHEN (qt.total_won_net_arr - s.won_net_arr)  > 0 
            THEN s.open_1plus_net_arr / (qt.total_won_net_arr - s.won_net_arr)                 
          ELSE NULL 
        END                                                                                 AS open_1plus_coverage_net_arr,
        CASE 
          WHEN (qt.total_won_net_arr - s.won_net_arr)  > 0 
            THEN s.open_3plus_net_arr / (qt.total_won_net_arr - s.won_net_arr) 
          ELSE NULL 
        END                                                                                 AS open_3plus_coverage_net_arr,
        CASE 
          WHEN (qt.total_won_net_arr - s.won_net_arr)  > 0 
            THEN s.open_4plus_net_arr / (qt.total_won_net_arr - s.won_net_arr) 
          ELSE NULL 
        END                                                                                 AS open_4plus_coverage_net_arr,
        
        CASE 
          WHEN  qt.total_won_net_arr  <> 0
            THEN s.won_net_arr / qt.total_won_net_arr
          ELSE NULL 
        END                                                                                 AS pacing_won_qtd_net_arr,

        CASE 
          WHEN qn.total_won_net_arr  <> 0
            THEN s.next_open_1plus_net_arr /  qn.total_won_net_arr 
          ELSE NULL 
        END                                                                                 AS next_open_1plus_coverage_net_arr,

        CASE 
          WHEN qn.total_won_net_arr  <> 0
            THEN s.next_open_3plus_net_arr /  qn.total_won_net_arr
          ELSE NULL
        END                                                                                 AS next_open_3plus_coverage_net_arr,

        s.created_and_won_same_quarter_net_arr,
        qt.total_created_and_won_net_arr,
         
        -- generated pipeline in quarter
        -- 20201206 - NF - Need to adjust for Vision ops messing historical perspective
        CASE 
         WHEN s.close_fiscal_quarter_name = 'FY21-Q2'
           AND s.snapshot_day_of_fiscal_quarter_normalised BETWEEN 45 AND 85
           THEN null 
         ELSE s.created_in_quarter_net_arr 
        END                                                                                         AS created_in_quarter_net_arr,

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        
        -- keys
        CASE 
            WHEN lower(coalesce(s.sales_qualified_source,'other')) = 'other'
                THEN 'other'
            ELSE lower(s.sales_team_cro_level) || '_' || lower(coalesce(s.sales_qualified_source,'other'))   
         END                                                                                            AS key_cro_sqs,

         -- last updated timstamp
         CURRENT_TIMESTAMP                                                                    AS last_updated_at
   FROM detail_segment s
   -- total per previous quarter
   LEFT JOIN total_per_quarter qt
       ON qt.sales_team_cro_level = s.sales_team_cro_level
        AND qt.close_fiscal_quarter_name = s.close_fiscal_quarter_name
        AND qt.sales_qualified_source = s.sales_qualified_source
   -- total per next quarter
   LEFT JOIN total_per_quarter qn
       ON qn.sales_team_cro_level = s.sales_team_cro_level
        AND qn.close_fiscal_quarter_name = s.next_close_fiscal_quarter_name
        AND qn.sales_qualified_source = s.sales_qualified_source
  -- and s.snapshot_day_of_fiscal_quarter = 1
   --and s.close_fiscal_quarter = 'FY21-Q3'