{{ config(alias='report_pipeline_velocity_quarter_with_targets') }}

WITH report_pipeline_velocity_quarter AS (
  
  SELECT *
  FROM {{ref('wk_sales_report_pipeline_velocity_quarter')}}  
  WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }}  

), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter              AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy                   AS current_fiscal_quarter_name,
                   day_of_fiscal_quarter_normalised         AS current_day_of_fiscal_quarter_normalised
   FROM date_details 
   WHERE date_actual = CURRENT_DATE
  
), sfdc_opportunity_xf AS (
  
  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  CROSS JOIN today_date
  WHERE is_excluded_flag = 0
    AND is_edu_oss = 0
    AND is_deleted = 0
    AND LOWER(deal_group) LIKE ANY ('%growth%','%new%')

), sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
    WHERE is_deleted = 0
      AND is_edu_oss = 0

), targets AS (
  
  SELECT *,
        sales_qualified_source_name AS sales_qualified_source,
        CONCAT(crm_user_sales_segment,'_',crm_user_region) AS sales_team_rd_asm_level,
        crm_user_sales_segment AS sales_team_cro_level,
          CASE 
          WHEN order_type_name = '3. Growth' 
              THEN '2. Growth'
          WHEN order_type_name = '1. New - First Order' 
              THEN '1. New'
            ELSE '3. Other'
         END                                                       AS deal_group
  FROM {{ref('mart_sales_funnel_target')}}   
  WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')

), report_pipeline_velocity AS (
  
  SELECT *
  FROM report_pipeline_velocity_quarter
  CROSS JOIN today_date
  WHERE is_excluded_flag = 0
    AND LOWER(deal_group) LIKE ANY ('%growth%','%new%')
 
), totals_per_quarter AS (
  
 SELECT 
        close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        COALESCE(sales_team_rd_asm_level,'NA')    AS sales_team_rd_asm_level,
        COALESCE(sales_team_cro_level,'NA')       AS sales_team_cro_level,
        COALESCE(sales_qualified_source,'NA')     AS sales_qualified_source,
        COALESCE(deal_group,'NA')                 AS deal_group,
        sum(net_arr)                              AS total_net_arr
   FROM sfdc_opportunity_snapshot_history_xf o
   WHERE (o.is_won = 1 OR (o.is_renewal = 1 AND o.is_lost = 1))
     AND o.close_fiscal_year >= 2020
     AND o.is_excluded_flag = 0
     AND o.is_deleted = 0
     AND o.close_fiscal_quarter_name = o.snapshot_fiscal_quarter_name
     AND o.snapshot_day_of_fiscal_quarter_normalised = 90
   GROUP BY 1,2,3,4,5,6
  
 ), targets_per_quarter AS (
  
  SELECT d.fiscal_quarter_name_fy AS close_fiscal_quarter_name,
   d.first_day_of_fiscal_quarter  AS close_fiscal_quarter_date,
   COALESCE(sales_team_rd_asm_level,'NA') AS sales_team_rd_asm_level,
   COALESCE(sales_team_cro_level,'NA') AS sales_team_cro_level,
   COALESCE(sales_qualified_source,'NA') AS sales_qualified_source,
   deal_group,
   SUM(allocated_target)         AS target_net_arr	
  FROM targets
  INNER JOIN  date_details d	
    ON d.date_actual = target_month	
  WHERE kpi_name = 'Net ARR'	
  GROUP BY 1,2,3,4,5,6
  HAVING target_net_arr > 0	
   

), consolidated_targets_totals AS (
  
  SELECT 
     base.close_fiscal_quarter_name,
     base.close_fiscal_quarter_date,
     base.sales_team_rd_asm_level,
     base.sales_team_cro_level,
     base.sales_qualified_source,
     base.deal_group,
     target.target_net_arr,
     total.total_net_arr,
     CASE
      WHEN today_date.current_fiscal_quarter_date <= base.close_fiscal_quarter_date
        THEN target.target_net_arr
      ELSE total.total_net_arr
     END                  AS adjusted_target_net_arr
  FROM (SELECT close_fiscal_quarter_name,
             close_fiscal_quarter_date,
             sales_team_rd_asm_level,
             sales_team_cro_level,
             sales_qualified_source,
             deal_group
        FROM targets_per_quarter
        UNION
        SELECT close_fiscal_quarter_name,
             close_fiscal_quarter_date,
             sales_team_rd_asm_level,
             sales_team_cro_level,
             sales_qualified_source,
             deal_group
        FROM totals_per_quarter) base
  CROSS JOIN today_date
  LEFT JOIN targets_per_quarter target
     ON target.close_fiscal_quarter_name = base.close_fiscal_quarter_name
      AND target.sales_team_rd_asm_level = base.sales_team_rd_asm_level
      AND target.sales_team_cro_level = base.sales_team_cro_level
      AND target.sales_qualified_source = base.sales_qualified_source
      AND target.deal_group = base.deal_group
  LEFT JOIN totals_per_quarter total
     ON total.close_fiscal_quarter_name = base.close_fiscal_quarter_name
      AND total.sales_team_rd_asm_level = base.sales_team_rd_asm_level
      AND total.sales_team_cro_level = base.sales_team_cro_level
      AND total.sales_qualified_source = base.sales_qualified_source
      AND total.deal_group = base.deal_group
  WHERE ((target.target_net_arr <> 0 
            AND today_date.current_fiscal_quarter_date <= base.close_fiscal_quarter_date)
    OR (total.total_net_arr <> 0 
            AND today_date.current_fiscal_quarter_date > base.close_fiscal_quarter_date))

), pipeline_summary AS (
  
  SELECT pv.close_fiscal_quarter_name,
         pv.close_fiscal_quarter_date,
         pv.close_day_of_fiscal_quarter_normalised,
  
         COALESCE(pv.sales_team_rd_asm_level,'NA') AS sales_team_rd_asm_level,
         COALESCE(pv.sales_team_cro_level,'NA')    AS sales_team_cro_level,
         COALESCE(pv.sales_qualified_source,'NA')  AS sales_qualified_source,
         COALESCE(pv.deal_group,'NA')              AS deal_group,
  
         SUM(CASE 
            WHEN pv.forecast_category_name != 'Omitted'
              AND pv.is_stage_1_plus = 1
              AND pv.is_open = 1 
              AND pv.net_arr is not null
                THEN pv.net_arr
            ELSE 0
          END)                                 AS open_stage_1_net_arr,
          SUM(CASE 
            WHEN pv.forecast_category_name != 'Omitted'
              AND pv.is_stage_3_plus = 1
              AND pv.is_open = 1 
              AND pv.net_arr is not null
                THEN pv.net_arr
            ELSE 0
          END)                                 AS open_stage_3_net_arr,
         SUM(CASE 
            WHEN pv.forecast_category_name != 'Omitted'
              AND pv.is_stage_4_plus = 1
              AND pv.is_open = 1 
              AND pv.net_arr is not null
                THEN pv.net_arr
            ELSE 0
          END)                                 AS open_stage_4_net_arr,
  
         SUM(CASE 
            WHEN (pv.is_won = 1 OR (pv.is_renewal = 1 AND pv.is_lost = 1))
              THEN pv.net_arr
            ELSE 0
         END)                                  AS won_net_arr
  FROM report_pipeline_velocity pv
  WHERE pv.close_fiscal_year >= 2020
     AND (pv.close_day_of_fiscal_quarter_normalised != pv.current_day_of_fiscal_quarter_normalised
          OR pv.close_fiscal_quarter_date != pv.current_fiscal_quarter_date)
  GROUP BY 1, 2,3,4,5,6,7
  UNION
   -- to have the same current values as in X-Ray
  SELECT 
    o.close_fiscal_quarter_name,
    o.close_fiscal_quarter_date,
    o.current_day_of_fiscal_quarter_normalised,

    o.sales_team_rd_asm_level,
    o.sales_team_cro_level,
    o.sales_qualified_source,
    o.deal_group,
  
   SUM(CASE 
        WHEN o.forecast_category_name != 'Omitted'
            AND o.is_stage_1_plus = 1
            AND o.is_open = 1 
            AND o.net_arr is not null
          THEN o.net_arr
        ELSE 0
      END)                                 AS open_stage_1_net_arr,
  
     SUM(CASE 
        WHEN o.forecast_category_name != 'Omitted'
            AND o.is_stage_3_plus = 1
            AND o.is_open = 1 
            AND o.net_arr is not null
          THEN o.net_arr
        ELSE 0
      END)                                 AS open_stage_3_net_arr,
  
     SUM(CASE 
        WHEN o.forecast_category_name != 'Omitted'
            AND o.is_stage_4_plus = 1
            AND o.is_open = 1 
            AND o.net_arr is not null
          THEN o.net_arr
        ELSE 0
      END)                                 AS open_stage_4_net_arr,
  
    SUM(CASE 
        WHEN (o.is_won = 1 OR (o.is_renewal = 1 AND o.is_lost = 1))
          THEN COALESCE(o.net_arr,0)
        ELSE 0
      END)                                 AS won_net_arr
  FROM sfdc_opportunity_xf o
  WHERE o.close_fiscal_quarter_name = o.current_fiscal_quarter_name
  GROUP BY 1, 2,3,4,5,6,7

), pipeline_velocity_with_targets_per_day AS (
  
  SELECT
  
    base.close_fiscal_quarter_name,
    base.close_fiscal_quarter_date,
    base.close_day_of_fiscal_quarter_normalised,

    base.sales_team_rd_asm_level,
    base.sales_team_cro_level,
    base.sales_qualified_source,
    base.deal_group,
  
    target.total_net_arr,
    target.target_net_arr,
    target.adjusted_target_net_arr,
  
    ps.open_stage_1_net_arr,
    ps.open_stage_3_net_arr,
    ps.open_stage_4_net_arr,
    ps.won_net_arr
    
  FROM (
     SELECT close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        close_day_of_fiscal_quarter_normalised,
        sales_team_rd_asm_level,
        sales_team_cro_level,
        sales_qualified_source,
        deal_group
      FROM pipeline_summary
      UNION
      SELECT close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        close_day_of_fiscal_quarter_normalised,
        sales_team_rd_asm_level,
        sales_team_cro_level,
        sales_qualified_source,
        deal_group
      FROM consolidated_targets_totals
      CROSS JOIN (SELECT DISTINCT close_day_of_fiscal_quarter_normalised
                FROM pipeline_summary) close_day) base
  LEFT JOIN  consolidated_targets_totals target  
    ON target.close_fiscal_quarter_name = base.close_fiscal_quarter_name
    AND target.sales_team_rd_asm_level = base.sales_team_rd_asm_level
    AND target.sales_team_cro_level = base.sales_team_cro_level
    AND target.sales_qualified_source = base.sales_qualified_source
    AND target.deal_group = base.deal_group
  LEFT JOIN  pipeline_summary ps  
    ON base.close_fiscal_quarter_name = ps.close_fiscal_quarter_name
    AND base.sales_team_rd_asm_level = ps.sales_team_rd_asm_level
    AND base.sales_team_cro_level = ps.sales_team_cro_level
    AND base.sales_qualified_source = ps.sales_qualified_source
    AND base.deal_group = ps.deal_group
    AND base.close_day_of_fiscal_quarter_normalised = ps.close_day_of_fiscal_quarter_normalised
  -- only consider quarters we have data in the snapshot history
  WHERE base.close_fiscal_quarter_date >= '2019-08-01'::DATE
  AND base.close_day_of_fiscal_quarter_normalised <= 90
)

SELECT *
FROM pipeline_velocity_with_targets_per_day