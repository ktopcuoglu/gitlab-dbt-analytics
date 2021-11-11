{{ config(alias='report_account_metrics_summary_year') }}
--https://gitlab.my.salesforce.com/0016100000g04uJAAQ
WITH date_details AS (

      SELECT * 
      --FROM  nfiguera_prod.workspace_sales.date_details
      FROM {{ ref('wk_sales_date_details') }} 
  
 ), sfdc_opportunity_xf AS (
  
    SELECT *
    --FROM nfiguera_prod.restricted_safe_workspace_sales.sfdc_opportunity_xf o
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}} o
    WHERE o.is_deleted = 0
      AND o.is_edu_oss = 0
      AND o.is_jihu_account = 0

 ), sfdc_opportunity_snapshot_xf AS (
  
    SELECT h.*
    --FROM nfiguera_prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf h
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}} h
    INNER JOIN date_details snapshot_date
      ON snapshot_date.date_actual = h.snapshot_date
    WHERE h.is_deleted = 0
      AND h.is_edu_oss = 0
      AND h.is_jihu_account = 0
      AND snapshot_date.day_of_fiscal_year_normalised = (SELECT DISTINCT day_of_fiscal_year_normalised 
                                                          FROM date_details
                                                          WHERE date_actual = DATEADD(day, -2, CURRENT_DATE))

  ), mart_arr AS (

    SELECT *
    --FROM prod.restricted_safe_common_mart_sales.mart_arr
    FROM {{ref('mart_arr')}} 


  ), dim_crm_account AS (

    SELECT *
    --FROM prod.restricted_safe_common.dim_crm_account
    FROM {{ref('dim_crm_account')}} 
      
  ), sfdc_accounts_xf AS (
    
    SELECT *
    --FROM prod.restricted_safe_legacy.sfdc_accounts_xf
    FROM {{ref('sfdc_accounts_xf')}} 

  ), stitch_account  AS (

    SELECT *
    --FROM raw.salesforce_stitch.account
    FROM {{ source('salesforce', 'account') }}


  ), sfdc_users_xf AS (

    SELECT *
    --FROM nfiguera_prod.workspace_sales.sfdc_users_xf
    FROM {{ref('wk_sales_sfdc_users_xf')}} 
    
    
  ), report_dates AS (
  
    SELECT DISTINCT fiscal_year             AS report_fiscal_year,
                    first_day_of_month      AS report_month_date
    FROM date_details 
    WHERE fiscal_year IN (2020,2021,2022)
        AND month_actual = month(CURRENT_DATE)
    
  ), account_year_key AS (

   SELECT DISTINCT a.dim_crm_account_id AS account_id,
        d.report_fiscal_year,
        d.report_month_date
  FROM dim_crm_account a
  CROSS JOIN report_dates d
     
  ), account_deal_path AS (
  
  -- Used to calculate cumulative Booked Net ARR as a way to identify the deal path of the account in general
  -- using Zuora would be better, but being time sensitive
  
     SELECT 
        o.snapshot_fiscal_year  AS report_fiscal_year,
        o.account_id,
        sum(o.booked_net_arr) AS cum_booked_net_arr,
        
        -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.booked_net_arr
            ELSE 0 END) AS cum_direct_booked_net_arr,
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.booked_net_arr
            ELSE 0 END) AS cum_channel_booked_net_arr
            
    FROM sfdc_opportunity_snapshot_xf o
    WHERE (is_won = 1 OR (is_renewal = 1 AND is_lost = 1))
    AND o.close_date < o.snapshot_date
    GROUP BY 1,2


  ), nfy_atr_base AS (
  
    SELECT 
        o.account_id,
        -- e.g. We want to show ATR on the previous FY
        d.fiscal_year - 1   AS report_fiscal_year,
        SUM(o.arr_basis)    AS nfy_sfdc_atr
    FROM sfdc_opportunity_xf o
    LEFT JOIN date_details d
      ON o.subscription_start_date = d.date_actual
    WHERE o.sales_type = 'Renewal'
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','00-Pre Opportunity')
      AND amount <> 0
      GROUP BY 1, 2
   
  ), fy_atr_base AS (
  
    SELECT 
        o.account_id,
        -- e.g. We want to show ATR on the previous FY
        d.fiscal_year       AS report_fiscal_year,
        SUM(o.arr_basis)    AS fy_sfdc_atr
    FROM sfdc_opportunity_xf o
    LEFT JOIN date_details d
      ON o.subscription_start_date = d.date_actual
    WHERE o.sales_type = 'Renewal'
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','00-Pre Opportunity')
      AND amount <> 0
      GROUP BY 1, 2 
    
-- Rolling 1 year Net ARR
), net_arr_ttm AS (
  
  SELECT 
      o.account_id,
      d.report_fiscal_year          AS report_fiscal_year,
      SUM(o.net_arr)                AS ttm_net_arr,
      SUM(CASE  
            WHEN  o.sales_qualified_source != 'Web Direct Generated' 
              THEN o.net_arr 
            ELSE 0 
          END)          AS ttm_non_web_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'Web Direct Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS ttm_web_direct_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'Channel Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS ttm_channel_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'SDR Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS ttm_sdr_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'AE Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS ttm_ae_sourced_net_arr,
      SUM(CASE 
            WHEN o.is_eligible_churn_contraction_flag = 1
                AND o.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Contract Reset','Contract Reset/Ramp Deal')
            THEN o.net_arr 
            ELSE 0 END) AS ttm_churn_contraction_net_arr,
  
       -- FO year
        SUM(CASE
            WHEN o.order_type_live = '1. New - First Order'
            THEN o.net_arr
            ELSE 0 END) AS ttm_fo_net_arr,
  
        -- New Connected year
        SUM(CASE
            WHEN o.order_type_live = '2. New - Connected'
            THEN o.net_arr
            ELSE 0 END) AS ttm_new_connected_net_arr,
           
        -- Growth year
        SUM(CASE
            WHEN o.order_type_live NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.net_arr
            ELSE 0 END) AS ttm_growth_net_arr,
  
        -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS ttm_direct_net_arr,
        
        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS ttm_channel_net_arr,
  
        SUM (o.calculated_deal_count)   AS ttm_deal_count,
  
          -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS ttm_direct_deal_count,
        
        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS ttm_channel_deal_count,
   
        -- deal path direct arr basis year
        -- 20211108 - Hacky way of getting to figure out if an account is channel or direct
        SUM(CASE
            WHEN o.deal_path != 'Channel'
                AND o.is_won = 1
            THEN o.arr_basis
            ELSE 0 END) AS ttm_direct_arr_basis,
        
        -- deal path channel arr basis, year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
                AND o.is_won = 1
            THEN o.arr_basis
            ELSE 0 END) AS ttm_channel_arr_basis
        
    FROM sfdc_opportunity_xf o
    CROSS JOIN report_dates d 
    WHERE o.close_date BETWEEN DATEADD(month, -12,DATE_TRUNC('month',d.report_month_date)) and 
      DATE_TRUNC('month',d.report_month_date)
      AND ((o.sales_type IN ('New Business','Add-On Business','Renewal')
              AND o.stage_name = 'Closed Won') 
            OR (o.sales_type = 'Renewal' AND o.stage_name = '8-Closed Lost'))
      AND o.net_arr <> 0

    GROUP BY 1, 2
   
  -- total booked net arr in fy
  ), total_net_arr_fiscal AS (
  
    SELECT
      o.account_id,
      o.close_fiscal_year   AS report_fiscal_year,
      SUM(o.net_arr)        AS fy_net_arr,
      SUM(CASE  
            WHEN  o.sales_qualified_source != 'Web Direct Generated' 
              THEN o.net_arr 
            ELSE 0 
          END)          AS fy_non_web_booked_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'Web Direct Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS fy_web_direct_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'Channel Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS fy_channel_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'SDR Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS fy_sdr_sourced_net_arr,
      SUM(CASE 
            WHEN o.sales_qualified_source = 'AE Generated' 
            THEN o.net_arr 
            ELSE 0 END) AS fy_ae_sourced_net_arr,
      SUM(CASE 
            WHEN o.is_eligible_churn_contraction_flag = 1
                AND o.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Contract Reset','Contract Reset/Ramp Deal')
            THEN o.net_arr 
            ELSE 0 END) AS fy_churn_contraction_net_arr,
        
        -- First Order year
        SUM(CASE
            WHEN o.order_type_live = '1. New - First Order'
            THEN o.net_arr
            ELSE 0 END) AS fy_fo_net_arr,
        
        -- New Connected year
        SUM(CASE
            WHEN o.order_type_live = '2. New - Connected'
            THEN o.net_arr
            ELSE 0 END) AS fy_new_connected_net_arr,
           
        -- Growth year
        SUM(CASE
            WHEN o.order_type_live NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.net_arr
            ELSE 0 END) AS fy_growth_net_arr,
        
        SUM(o.calculated_deal_count)   AS fy_deal_count,
    
              -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS fy_direct_net_arr,
        
        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.net_arr
            ELSE 0 END) AS fy_channel_net_arr,
    
         -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_direct_deal_count,
        
        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_channel_deal_count
    
    FROM sfdc_opportunity_xf o
    WHERE ((o.sales_type IN ('New Business','Add-On Business','Renewal')
              AND o.stage_name = 'Closed Won') 
            OR (o.sales_type = 'Renewal' AND o.stage_name = '8-Closed Lost')
          )
      AND o.net_arr <> 0
      GROUP BY 1,2
  
  -- Total open pipeline at the same point in previous fiscal years (total open pipe)
  ), op_forward_one_year AS (

    SELECT 
      h.account_id,
      h.snapshot_fiscal_year AS report_fiscal_year,
      sum(h.net_arr)         AS open_pipe
    FROM sfdc_opportunity_snapshot_xf h
    WHERE h.close_date > h.snapshot_date
      AND h.forecast_category_name NOT IN  ('Omitted','Closed')
      AND h.stage_name IN ('1-Discovery','2-Scoping','3-Technical','Evaluation','4-Proposal','5-Negotiating','6-Awaiting Signature','7-Closing')
      AND h.order_type_stamped != '7. PS / Other'
      AND h.net_arr != 0
      AND h.is_eligible_open_pipeline_flag = 1
      GROUP BY 1,2
  
  -- Last 12 months pipe gen at same point of time in the year
  ), pg_last_12_months AS (

    SELECT 
      h.account_id,
      h.snapshot_fiscal_year AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_last_12m_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'Web Direct Generated' 
            THEN h.net_arr 
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'Channel Generated' 
            THEN h.net_arr 
            ELSE 0 END)              AS pg_last_12m_channel_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'SDR Generated' 
            THEN h.net_arr 
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'AE Generated' 
            THEN h.net_arr 
            ELSE 0 END)              AS pg_last_12m_ae_sourced_net_arr
    FROM sfdc_opportunity_snapshot_xf h
       -- pipeline created within the last 12 months
      WHERE h.pipeline_created_date > dateadd(month,-12,h.snapshot_date)
      AND h.pipeline_created_date <= h.snapshot_date
      AND h.stage_name IN ('1-Discovery','2-Scoping','3-Technical','Evaluation','4-Proposal','5-Negotiating','6-Awaiting Signature','7-Closing','Closed Won','8-Closed Lost')
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
      GROUP BY 1,2
    
  -- Pipe generation at the same point in time in the fiscal year
  ), pg_ytd AS (

    SELECT
      h.account_id,
      h.net_arr_created_fiscal_year  AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_ytd_net_arr,
       SUM(CASE 
            WHEN h.sales_qualified_source = 'Web Direct Generated' 
            THEN h.net_arr 
            ELSE 0 END) AS pg_ytd_web_direct_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'Channel Generated' 
            THEN h.net_arr 
            ELSE 0 END) AS pg_ytd_channel_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'SDR Generated' 
            THEN h.net_arr 
            ELSE 0 END) AS pg_ytd_sdr_sourced_net_arr,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'AE Generated' 
            THEN h.net_arr 
            ELSE 0 END) AS pg_ytd_ae_sourced_net_arr
    FROM sfdc_opportunity_snapshot_xf h
      -- pipeline created within the fiscal year
      WHERE h.snapshot_fiscal_year = h.net_arr_created_fiscal_year
      AND h.stage_name IN ('1-Discovery','2-Scoping','3-Technical','Evaluation','4-Proposal','5-Negotiating','6-Awaiting Signature','7-Closing','Closed Won','8-Closed Lost')
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
      AND h.net_arr > 0
      GROUP BY 1,2

  -- ARR at the same point in time in Fiscal Year
  ), arr_at_same_month AS (

    SELECT 
        mrr.dim_crm_account_id AS account_id,
        mrr_date.fiscal_year   AS report_fiscal_year,
    --    ultimate_parent_account_id,
        sum(mrr.mrr)      AS mrr,
        sum(mrr.arr)      AS arr
    FROM mart_arr mrr
      INNER JOIN date_details mrr_date
        ON mrr.arr_month = mrr_date.date_actual
    WHERE mrr_date.month_actual =  (SELECT DISTINCT month_actual 
                                      FROM date_details
                                      WHERE date_actual = DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE)))
    GROUP BY 1,2

), country AS (

  SELECT DISTINCT 
    billingcountry        AS countryname, 
    billingcountrycode    AS countrycode
  FROM stitch_account
  
), consolidated_accounts AS (

SELECT 
  ak.report_fiscal_year,
  a.dim_crm_account_id          AS account_id,
  a.crm_account_name            AS account_name,
  a.dim_parent_crm_account_id   AS upa_account_id,
  a.parent_crm_account_name     AS upa_account_name,
  a.crm_account_owner           AS account_owner,
  a.dim_crm_user_id             AS account_owner_id,
  upa_owner.name                AS upa_account_owner,
  upa_owner.user_id             AS upa_account_owner_id,
  raw.forbes_2000_rank__c       AS account_forbes_rank,
  a.crm_account_billing_country AS account_country,
  coalesce(upa_c.countryname,REPLACE(REPLACE(REPLACE(upa_leg.tsp_address_country,'The Netherlands','Netherlands'),'Russian Federation','Russia'),'Russia','Russian Federation'))    AS upa_country,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_STATE__C        AS upa_state,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_CITY__C         AS upa_city,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_POSTAL_CODE__C  AS upa_zip_code,
  u.user_geo                                    AS account_user_geo,
  u.user_region                                 AS account_user_region,
  u.user_segment                                AS account_user_segment,
  u.user_area                                   AS account_user_area,
  u.role_name                                   AS account_owner_role,
  upa_owner.user_geo                            AS upa_user_geo,
  upa_owner.user_region                         AS upa_user_region,
  upa_owner.user_segment                        AS upa_user_segment,
  upa_owner.user_area                           AS upa_user_area,
  upa_owner.role_name                           AS upa_user_role,
  upa.crm_account_industry                      AS upa_industry,
  raw.POTENTIAL_USERS__C                        AS potential_users,
  raw.Number_of_Licenses_This_Account__c        AS licenses,
  raw.Decision_Maker_Count_Linkedin__c          AS linkedin_developer,
  raw.ZI_NUMBER_OF_DEVELOPERS__C                AS zi_developers,
  raw.zi_revenue__c                             AS zi_revenue,
  raw.ACCOUNT_DEMOGRAPHICS_EMPLOYEE_COUNT__C    AS employees,
  
  -- LAM
  COALESCE(raw.potential_arr_lam__c,0)            AS potential_arr_lam,
  COALESCE(raw.potential_carr_this_account__c,0)  AS potential_carr_this_account,
  
  
  -- atr for current fy
  COALESCE(fy_atr_base.fy_sfdc_atr,0)        AS fy_sfdc_atr,
  -- next fiscal year atr base reported at fy
  COALESCE(nfy_atr_base.nfy_sfdc_atr,0)      AS nfy_sfdc_atr,
  
  -- arr by fy
  COALESCE(arr.arr,0)                        AS arr,
  
  adp.cum_direct_booked_net_arr,
  adp.cum_channel_booked_net_arr,
  
    -- identify the main source of net arr, if direct or channel driven
  CASE
    WHEN adp.cum_direct_booked_net_arr > adp.cum_channel_booked_net_arr
        THEN 0
    ELSE COALESCE(arr.arr,0)
  END                                                       AS arr_channel,
  
    -- identify the main source of net arr, if direct or channel driven
  CASE
    WHEN adp.cum_direct_booked_net_arr > adp.cum_channel_booked_net_arr
        THEN COALESCE(arr.arr,0)
    ELSE 0
  END                                                       AS arr_direct,
  
  
  CASE 
    WHEN COALESCE(arr.arr,0) > 5000
    THEN 1
    ELSE 0 
  END                                        AS is_over_5k_customer_flag,
  CASE 
    WHEN COALESCE(arr.arr,0) > 10000
    THEN 1
    ELSE 0 
  END                                        AS is_over_10k_customer_flag,
  CASE 
    WHEN COALESCE(arr.arr,0) > 50000
    THEN 1
    ELSE 0 
  END                                       AS is_over_50k_customer_flag, 
  
  CASE 
    WHEN COALESCE(arr.arr,0) > 500000
    THEN 1
    ELSE 0 
  END                                       AS is_over_500k_customer_flag, 
  
  -- rolling last 12 months bokked net arr
  COALESCE(net_arr_ttm.ttm_net_arr,0)                       AS ttm_net_arr,
  COALESCE(net_arr_ttm.ttm_non_web_net_arr,0)               AS ttm_non_web_net_arr,
  COALESCE(net_arr_ttm.ttm_web_direct_sourced_net_arr,0)    AS ttm_web_direct_sourced_net_arr,
  COALESCE(net_arr_ttm.ttm_channel_sourced_net_arr,0)       AS ttm_channel_sourced_net_arr,
  COALESCE(net_arr_ttm.ttm_sdr_sourced_net_arr,0)           AS ttm_sdr_sourced_net_arr,
  COALESCE(net_arr_ttm.ttm_ae_sourced_net_arr,0)            AS ttm_ae_sourced_net_arr,
  COALESCE(net_arr_ttm.ttm_churn_contraction_net_arr,0)     AS ttm_churn_contraction_net_arr,
  COALESCE(net_arr_ttm.ttm_fo_net_arr,0)                    AS ttm_fo_net_arr,
  COALESCE(net_arr_ttm.ttm_new_connected_net_arr,0)         AS ttm_new_connected_net_arr,
  COALESCE(net_arr_ttm.ttm_growth_net_arr,0)                AS ttm_growth_net_arr,
  COALESCE(net_arr_ttm.ttm_deal_count,0)                    AS ttm_deal_count,
  COALESCE(net_arr_ttm.ttm_direct_net_arr,0)                AS ttm_direct_net_arr,
  COALESCE(net_arr_ttm.ttm_channel_net_arr,0)               AS ttm_channel_net_arr,
  COALESCE(net_arr_ttm.ttm_channel_net_arr,0)  - COALESCE(net_arr_ttm.ttm_channel_sourced_net_arr,0)   AS ttm_channel_co_sell_net_arr,
  COALESCE(net_arr_ttm.ttm_direct_deal_count,0)             AS ttm_direct_deal_count,
  COALESCE(net_arr_ttm.ttm_channel_deal_count,0)            AS ttm_channel_deal_count,
  
  net_arr_ttm.ttm_direct_arr_basis,  
  net_arr_ttm.ttm_channel_arr_basis,
  
  -- identify the main source of net arr, if direct or channel driven
  CASE
    WHEN adp.cum_direct_booked_net_arr > adp.cum_channel_booked_net_arr
        THEN 0
    ELSE 1
  END                                                       AS ttm_is_channel_account_flag,
  
  -- fy booked net arr
  COALESCE(net_arr_fiscal.fy_net_arr,0)                     AS fy_net_arr,
  COALESCE(net_arr_fiscal.fy_web_direct_sourced_net_arr,0)  AS fy_web_direct_sourced_net_arr,
  COALESCE(net_arr_fiscal.fy_channel_sourced_net_arr,0)     AS fy_channel_sourced_net_arr,
  COALESCE(net_arr_fiscal.fy_sdr_sourced_net_arr,0)         AS fy_sdr_sourced_net_arr,
  COALESCE(net_arr_fiscal.fy_ae_sourced_net_arr,0)          AS fy_ae_sourced_net_arr,
  COALESCE(net_arr_fiscal.fy_churn_contraction_net_arr,0)   AS fy_churn_contraction_net_arr,
  COALESCE(net_arr_fiscal.fy_fo_net_arr,0)                  AS fy_fo_net_arr,
  COALESCE(net_arr_fiscal.fy_new_connected_net_arr,0)       AS fy_new_connected_net_arr,
  COALESCE(net_arr_fiscal.fy_growth_net_arr,0)              AS fy_growth_net_arr,
  COALESCE(net_arr_fiscal.fy_deal_count,0)                  AS fy_deal_count,
  COALESCE(net_arr_fiscal.fy_direct_net_arr,0)              AS fy_direct_net_arr,
  COALESCE(net_arr_fiscal.fy_channel_net_arr,0)             AS fy_channel_net_arr,
  COALESCE(net_arr_fiscal.fy_direct_deal_count,0)           AS fy_direct_deal_count,
  COALESCE(net_arr_fiscal.fy_channel_deal_count,0)          AS fy_channel_deal_count,
   
  -- open pipe forward looking
  COALESCE(op.open_pipe,0)                                  AS open_pipe,
  
  -- pipe generation
  COALESCE(pg.pg_ytd_net_arr,0)                             AS pg_ytd_net_arr,
  COALESCE(pg.pg_ytd_web_direct_sourced_net_arr,0)          AS pg_ytd_web_direct_sourced_net_arr,
  COALESCE(pg.pg_ytd_channel_sourced_net_arr,0)             AS pg_ytd_channel_sourced_net_arr,
  COALESCE(pg.pg_ytd_sdr_sourced_net_arr,0)                 AS pg_ytd_sdr_sourced_net_arr,
  COALESCE(pg.pg_ytd_ae_sourced_net_arr,0)                  AS pg_ytd_ae_sourced_net_arr,
  
  COALESCE(pg_ly.pg_last_12m_net_arr,0)                     AS pg_last_12m_net_arr,
  COALESCE(pg_ly.pg_last_12m_web_direct_sourced_net_arr,0)  AS pg_last_12m_web_direct_sourced_net_arr,
  COALESCE(pg_ly.pg_last_12m_channel_sourced_net_arr,0)     AS pg_last_12m_channel_sourced_net_arr,
  COALESCE(pg_ly.pg_last_12m_sdr_sourced_net_arr,0)         AS pg_last_12m_sdr_sourced_net_arr,
  COALESCE(pg_ly.pg_last_12m_ae_sourced_net_arr,0)          AS pg_last_12m_ae_sourced_net_arr

  
FROM account_year_key ak
  INNER JOIN dim_crm_account a
    ON ak.account_id = a.dim_crm_account_id
  LEFT JOIN stitch_account raw
    ON ak.account_id = raw.ACCOUNT_ID_18__C
  LEFT JOIN stitch_account uparaw
    ON a.dim_parent_crm_account_id = uparaw.ACCOUNT_ID_18__C
  LEFT JOIN country upa_c
    ON uparaw.ACCOUNT_DEMOGRAPHICS_UPA_COUNTRY__C = upa_c.countrycode
  LEFT JOIN sfdc_users_xf u 
    ON a.dim_crm_user_id = u.user_id
  LEFT JOIN dim_crm_account upa
    ON a.dim_parent_crm_account_id = upa.dim_crm_account_id
  LEFT JOIN sfdc_accounts_xf upa_leg
    ON upa_leg.account_id = upa.dim_crm_account_id
  LEFT JOIN sfdc_users_xf upa_owner 
    ON a.dim_crm_user_id = upa_owner.user_id
  LEFT JOIN fy_atr_base
    ON a.dim_crm_account_id = fy_atr_base.account_id
    AND fy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN nfy_atr_base
    ON a.dim_crm_account_id = nfy_atr_base.account_id
    AND nfy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN net_arr_ttm
    ON a.dim_crm_account_id = net_arr_ttm.account_id
    AND net_arr_ttm.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN op_forward_one_year op
    ON a.dim_crm_account_id = op.account_id
     AND op.report_fiscal_year = ak.report_fiscal_year 
  LEFT JOIN pg_ytd pg
    ON a.dim_crm_account_id = pg.account_id
    AND pg.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_last_12_months pg_ly
    ON a.dim_crm_account_id = pg_ly.account_id
    AND pg_ly.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN arr_at_same_month arr
    ON a.dim_crm_account_id = arr.account_id
    AND arr.report_fiscal_year = ak.report_fiscal_year 
  LEFT JOIN total_net_arr_fiscal net_arr_fiscal
    ON a.dim_crm_account_id = net_arr_fiscal.account_id
    AND net_arr_fiscal.report_fiscal_year = ak.report_fiscal_year 
  LEFT JOIN account_deal_path adp
    ON adp.account_id = ak.account_id
    AND adp.report_fiscal_year = ak.report_fiscal_year

), consolidated_upa AS (

SELECT 
  report_fiscal_year,
  upa_account_id,
  upa_account_name,
  upa_account_owner,
  upa_account_owner_id,
  upa_country,
  upa_state,
  upa_city,
  upa_zip_code,
  upa_user_geo,
  upa_user_region,
  upa_user_segment,
  upa_user_area,
  upa_user_role,
  upa_industry,
  SUM(CASE WHEN account_forbes_rank IS NOT NULL THEN 1 ELSE 0 END)   AS count_forbes_accounts,
  MIN(account_forbes_rank)      AS forbes_rank,
  MAX(potential_users)          AS potential_users,
  MAX(licenses)                 AS licenses,
  MAX(linkedin_developer)       AS linkedin_developer,
  MAX(zi_developers)            AS zi_developers,
  MAX(zi_revenue)               AS zi_revenue,
  MAX(employees)                AS employees,
  
  -- LAM
  MAX(potential_arr_lam)            AS potential_arr_lam,
  MAX(potential_carr_this_account)  AS potential_carr_this_account,
  
  -- atr for current fy
  SUM(fy_sfdc_atr)  AS fy_sfdc_atr,
  -- next fiscal year atr base reported at fy
  SUM(nfy_sfdc_atr) AS nfy_sfdc_atr,
  
  -- arr by fy
  SUM(arr) AS arr,
  MAX(is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
  MAX(is_over_10k_customer_flag)    AS is_over_10k_customer_flag,    
  MAX(is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
  MAX(is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
  SUM(is_over_5k_customer_flag)     AS count_over_5k_customers,
  SUM(is_over_10k_customer_flag)    AS count_over_10k_customers,
  SUM(is_over_50k_customer_flag)    AS count_over_50k_customers,
  SUM(is_over_500k_customer_flag)   AS count_over_500k_customers,

  
  -- rolling last 12 months bokked net arr
  SUM(ttm_net_arr)                      AS ttm_net_arr,
  SUM(ttm_non_web_net_arr)              AS ttm_non_web_net_arr,
  SUM(ttm_web_direct_sourced_net_arr)   AS ttm_web_direct_sourced_net_arr,
  SUM(ttm_channel_sourced_net_arr)      AS ttm_channel_sourced_net_arr,
  SUM(ttm_sdr_sourced_net_arr)          AS ttm_sdr_sourced_net_arr,
  SUM(ttm_ae_sourced_net_arr)           AS ttm_ae_sourced_net_arr,
  SUM(ttm_churn_contraction_net_arr)    AS ttm_churn_contraction_net_arr,
  SUM(ttm_fo_net_arr)               AS ttm_fo_net_arr,
  SUM(ttm_new_connected_net_arr)    AS ttm_new_connected_net_arr,
  SUM(ttm_growth_net_arr)           AS ttm_growth_net_arr,
  SUM(ttm_deal_count)               AS ttm_deal_count,
  SUM(ttm_direct_net_arr)           AS ttm_direct_net_arr,
  SUM(ttm_channel_net_arr)          AS ttm_channel_net_arr,
  SUM(ttm_direct_deal_count)        AS ttm_direct_deal_count,
  SUM(ttm_channel_deal_count)       AS ttm_channel_deal_count,
   
  -- fy booked net arr
  SUM(fy_net_arr)                   AS fy_net_arr,
  SUM(fy_web_direct_sourced_net_arr) AS fy_web_direct_sourced_net_arr,
  SUM(fy_channel_sourced_net_arr)   AS fy_channel_sourced_net_arr,
  SUM(fy_sdr_sourced_net_arr)       AS fy_sdr_sourced_net_arr,
  SUM(fy_ae_sourced_net_arr)        AS fy_ae_sourced_net_arr,
  SUM(fy_churn_contraction_net_arr) AS fy_churn_contraction_net_arr,
  SUM(fy_fo_net_arr)                AS fy_fo_net_arr,
  SUM(fy_new_connected_net_arr)     AS fy_new_connected_net_arr,
  SUM(fy_growth_net_arr)            AS fy_growth_net_arr,
  SUM(fy_deal_count)                AS fy_deal_count,
  SUM(fy_direct_net_arr)            AS fy_direct_net_arr,
  SUM(fy_channel_net_arr)           AS fy_channel_net_arr,
  SUM(fy_direct_deal_count)         AS fy_direct_deal_count,
  SUM(fy_channel_deal_count)        AS fy_channel_deal_count,
  
  -- open pipe forward looking
  SUM(open_pipe)                    AS open_pipe,
  
  -- pipe generation
  SUM(pg_ytd_net_arr) AS pg_ytd_net_arr,
  SUM(pg_ytd_web_direct_sourced_net_arr)    AS pg_ytd_web_direct_sourced_net_arr,
  SUM(pg_ytd_channel_sourced_net_arr)       AS pg_ytd_channel_sourced_net_arr,
  SUM(pg_ytd_sdr_sourced_net_arr)           AS pg_ytd_sdr_sourced_net_arr,
  SUM(pg_ytd_ae_sourced_net_arr)            AS pg_ytd_ae_sourced_net_arr,
  
  SUM(pg_last_12m_net_arr) AS pg_last_12m_net_arr,
  SUM(pg_last_12m_web_direct_sourced_net_arr)   AS pg_last_12m_web_direct_sourced_net_arr,
  SUM(pg_last_12m_channel_sourced_net_arr)      AS pg_last_12m_channel_sourced_net_arr,
  SUM(pg_last_12m_sdr_sourced_net_arr)          AS pg_last_12m_sdr_sourced_net_arr,
  SUM(pg_last_12m_ae_sourced_net_arr)           AS pg_last_12m_ae_sourced_net_arr

  
FROM consolidated_accounts
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
 )
select *
from consolidated_accounts