{{ config(alias='report_account_metrics_summary_year') }}

WITH date_details AS (

      SELECT * 
      --FROM  prod.workspace_sales.date_details
      FROM {{ ref('wk_sales_date_details') }} 
  
 ), sfdc_opportunity_xf AS (
  
    SELECT *
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
    WHERE is_deleted = 0
      AND is_edu_oss = 0
      AND is_jihu_account = 0

 ), sfdc_opportunity_snapshot_xf AS (
  
    SELECT h.*
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf h
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}} h
    INNER JOIN date_details snapshot_date
      ON snapshot_date.date_actual = h.snapshot_date
    WHERE h.is_deleted = 0
      AND h.is_edu_oss = 0
      AND h.is_jihu_account = 0
      AND snapshot_date.day_of_fiscal_year_normalised = (SELECT DISTINCT day_of_fiscal_year_normalised 
                                                          FROM date_details
                                                          WHERE date_actual = DATEADD(day, -2, CURRENT_DATE))

 ), stitch_subscription AS (
  
    SELECT 
        s.id AS subscription_id,
        CASE
            WHEN s.invoiceownerid != s.accountid
                THEN 1
            ELSE 0 
        END                                 AS is_channel_arr_flag
    --FROM raw.zuora_stitch.subscription s 
    FROM {{ source('zuora', 'subscription') }} s 
 
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
    FROM prod.restricted_safe_legacy.sfdc_accounts_xf
    --FROM {{ref('sfdc_accounts_xf')}} 

  ), stitch_account  AS (

    SELECT *
    FROM raw.salesforce_stitch.account
    --FROM {{ source('salesforce', 'account') }}


  ), sfdc_users_xf AS (

    SELECT *
    --FROM prod.workspace_sales.sfdc_users_xf
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
    
   ), ttm_atr_base AS (
  
    SELECT 
        o.account_id,
        -- e.g. We want to show ATR on the previous FY
        d.report_fiscal_year        AS report_fiscal_year,
        SUM(o.arr_basis)            AS ttm_atr
    FROM sfdc_opportunity_xf o
        CROSS JOIN report_dates d 
    WHERE o.sales_type = 'Renewal'
    AND o.subscription_start_date BETWEEN DATEADD(month, -12,DATE_TRUNC('month',d.report_month_date)) 
        AND DATE_TRUNC('month',d.report_month_date)
      AND o.stage_name NOT IN ('9-Unqualified','10-Duplicate','00-Pre Opportunity')
      AND o.amount <> 0
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
  
        SUM (CASE
            WHEN o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS ttm_deal_count,
  
          SUM (CASE
            WHEN o.is_renewal = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS ttm_renewal_deal_count,
  
        SUM(CASE 
            WHEN o.is_eligible_churn_contraction_flag = 1
                AND o.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Contract Reset','Contract Reset/Ramp Deal')
            THEN o.calculated_deal_count 
            ELSE 0 END) AS ttm_churn_contraction_deal_count,
  
          -- deal path direct year
        SUM(CASE
            WHEN o.deal_path != 'Channel'
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS ttm_direct_deal_count,
        
        -- deal path channel year
        SUM(CASE
            WHEN o.deal_path = 'Channel'
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS ttm_channel_deal_count
          
    FROM sfdc_opportunity_xf o
    CROSS JOIN report_dates d 
    WHERE o.close_date BETWEEN DATEADD(month, -12,DATE_TRUNC('month',d.report_month_date)) and 
      DATE_TRUNC('month',d.report_month_date)
      AND (o.stage_name = 'Closed Won' 
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
    WHERE (o.stage_name = 'Closed Won'
            OR (o.sales_type = 'Renewal' AND o.stage_name = '8-Closed Lost'))
      AND o.net_arr <> 0
      GROUP BY 1,2
  
  -- Total open pipeline at the same point in previous fiscal years (total open pipe)
  ), op_forward_one_year AS (

    SELECT 
      h.account_id,
      h.snapshot_fiscal_year        AS report_fiscal_year,
      SUM(h.net_arr)                AS open_pipe,
      SUM(h.calculated_deal_count)   AS count_open_deals
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
            ELSE 0 END)              AS pg_last_12m_ae_sourced_net_arr,
    
      SUM(CASE 
            WHEN h.sales_qualified_source = 'Web Direct Generated' 
            THEN h.calculated_deal_count 
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_deal_count,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'Channel Generated' 
            THEN h.calculated_deal_count 
            ELSE 0 END)              AS pg_last_12m_channel_sourced_deal_count,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'SDR Generated' 
            THEN h.calculated_deal_count 
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_deal_count,
      SUM(CASE 
            WHEN h.sales_qualified_source = 'AE Generated' 
            THEN h.calculated_deal_count 
            ELSE 0 END)              AS pg_last_12m_ae_sourced_deal_count
    
    
    
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
        SUM(mrr.mrr)      AS mrr,
        SUM(mrr.arr)      AS arr,
        SUM(CASE 
                WHEN sub.is_channel_arr_flag = 1
                    THEN mrr.arr
                ELSE 0
            END)          AS reseller_arr,
        SUM(CASE 
                WHEN  sub.is_channel_arr_flag = 0
                    THEN mrr.arr
                ELSE 0
            END)          AS direct_arr,
    
    
       SUM(CASE 
                WHEN  (mrr.product_tier_name LIKE '%Starter%'
                        OR mrr.product_tier_name LIKE '%Bronze%')
                    THEN mrr.arr
                ELSE 0
            END)          AS product_starter_arr,
    
    
        SUM(CASE 
                WHEN  mrr.product_tier_name LIKE '%Premium%'
                    THEN mrr.arr
                ELSE 0
            END)          AS product_premium_arr,
        SUM(CASE 
                WHEN  mrr.product_tier_name LIKE '%Ultimate%'
                    THEN mrr.arr
                ELSE 0
            END)          AS product_ultimate_arr, 
    
        SUM(CASE 
                WHEN  mrr.product_tier_name LIKE '%Self-Managed%'
                    THEN mrr.arr
                ELSE 0
            END)          AS delivery_self_managed_arr,
        SUM(CASE 
                WHEN  mrr.product_tier_name LIKE '%SaaS%'
                    THEN mrr.arr
                ELSE 0
            END)          AS delivery_saas_arr
      
    FROM mart_arr mrr
      INNER JOIN date_details mrr_date
        ON mrr.arr_month = mrr_date.date_actual
      INNER JOIN stitch_subscription sub
        ON sub.subscription_id = mrr.dim_subscription_id
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
  a.account_id                      AS account_id,
  a.account_name                    AS account_name,
  a.ultimate_parent_account_id      AS upa_id,
  a.ultimate_parent_account_name    AS upa_name,
  u.name                            AS account_owner_name,
  a.owner_id                        AS account_owner_id,
 -- u.start_date                      AS account_owner_start_date, 
  trim(u.employee_number)          AS account_owner_employee_number,          
 -- LEAST(12,datediff(month,u.start_date,ak.report_month_date)) AS account_months_in_year,
  upa_owner.name                    AS upa_owner_name,
  upa_owner.user_id                 AS upa_owner_id,
--  upa_owner.start_date            AS upa_owner_start_date,
  trim(upa_owner.employee_number)  AS upa_owner_employee_number, 
 -- LEAST(12,datediff(month,upa_owner.start_date,ak.report_month_date)) AS upa_months_in_year,
  raw.forbes_2000_rank__c           AS account_forbes_rank,
  a.billing_country                 AS account_country,
  coalesce(upa_c.countryname,REPLACE(REPLACE(REPLACE(upa.tsp_address_country,'The Netherlands','Netherlands'),'Russian Federation','Russia'),'Russia','Russian Federation'))    AS upa_country,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_STATE__C        AS upa_state,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_CITY__C         AS upa_city,
  uparaw.ACCOUNT_DEMOGRAPHICS_UPA_POSTAL_CODE__C  AS upa_zip_code,
  u.user_geo                                    AS account_user_geo,
  u.user_region                                 AS account_user_region,
  u.user_segment                                AS account_user_segment,
  u.user_area                                   AS account_user_area,
  u.role_name                                   AS account_owner_role,
  a.industry                                    AS account_industry,
  upa_owner.user_geo                            AS upa_user_geo,
  upa_owner.user_region                         AS upa_user_region,
  upa_owner.user_segment                        AS upa_user_segment,
  upa_owner.user_area                           AS upa_user_area,
  upa_owner.role_name                           AS upa_user_role,
  upa.industry                                  AS upa_industry,
  coalesce(raw.POTENTIAL_USERS__C,0)                            AS potential_users,
  coalesce(raw.Number_of_Licenses_This_Account__c,0)            AS licenses,
  coalesce(raw.Decision_Maker_Count_Linkedin__c,0)              AS linkedin_developer,
  coalesce(raw.ZI_NUMBER_OF_DEVELOPERS__C,0)                    AS zi_developers,
  coalesce(raw.zi_revenue__c,0)                                 AS zi_revenue,
  coalesce(raw.ACCOUNT_DEMOGRAPHICS_EMPLOYEE_COUNT__C,0)        AS employees,
  coalesce(raw.Aggregate_Developer_Count__c,0)                  AS upa_aggregate_dev_count,
  LEAST(50000,GREATEST(coalesce(raw.Number_of_Licenses_This_Account__c,0),COALESCE(raw.POTENTIAL_USERS__C,raw.Decision_Maker_Count_Linkedin__c,raw.ZI_NUMBER_OF_DEVELOPERS__C,raw.ZI_NUMBER_OF_DEVELOPERS__C,0)))  AS calculated_developer_count,
  
  a.technical_account_manager_date,              
  a.technical_account_manager                   AS technical_account_manager_name,
  
  CASE
    WHEN a.technical_account_manager IS NOT NULL
        THEN 1
    ELSE 0
  END                                           AS has_technical_account_manager_flag,
  
  a.health_score_color                          AS account_health_score_color,
  a.health_number                               AS account_health_number,
  
  -- LAM
  --COALESCE(raw.potential_arr_lam__c,0)            AS potential_arr_lam,
 -- COALESCE(raw.potential_carr_this_account__c,0)  AS potential_carr_this_account,
  
  
  -- atr for current fy
  COALESCE(fy_atr_base.fy_sfdc_atr,0)           AS fy_sfdc_atr,
  -- next fiscal year atr base reported at fy
  COALESCE(nfy_atr_base.nfy_sfdc_atr,0)         AS nfy_sfdc_atr,
  -- last 12 months ATR
  COALESCE(ttm_atr.ttm_atr,0)                   AS ttm_atr,
  
  -- arr by fy
  COALESCE(arr.arr,0)                           AS arr,
  
  COALESCE(arr.reseller_arr,0)                  AS arr_channel,
  COALESCE(arr.direct_arr,0)                    AS arr_direct,
  
  COALESCE(arr.product_starter_arr,0)           AS product_starter_arr,
  COALESCE(arr.product_premium_arr,0)           AS product_premium_arr,
  COALESCE(arr.product_ultimate_arr,0)          AS product_ultimate_arr, 
  
  
  CASE
    WHEN COALESCE(arr.product_ultimate_arr,0) > COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0) 
        THEN 1
    ELSE 0
  END                                           AS is_ultimate_customer_flag,
  
  CASE
    WHEN COALESCE(arr.product_ultimate_arr,0) < COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0) 
        THEN 1
    ELSE 0
  END                                           AS is_premium_customer_flag,
           
  COALESCE(arr.delivery_self_managed_arr,0)     AS delivery_self_managed_arr,
  COALESCE(arr.delivery_saas_arr,0)             AS delivery_saas_arr,
  
  
  -- accounts counts
  CASE 
    WHEN COALESCE(arr.arr,0) = 0
    THEN 1
    ELSE 0 
  END                                           AS is_prospect_flag,
  
  CASE 
    WHEN COALESCE(arr.arr,0) > 0
    THEN 1
    ELSE 0 
  END                                           AS is_customer_flag,
  
  CASE 
    WHEN COALESCE(arr.arr,0) > 5000
    THEN 1
    ELSE 0 
  END                                           AS is_over_5k_customer_flag,
  CASE 
    WHEN COALESCE(arr.arr,0) > 10000
    THEN 1
    ELSE 0 
  END                                           AS is_over_10k_customer_flag,
  CASE 
    WHEN COALESCE(arr.arr,0) > 50000
    THEN 1
    ELSE 0 
  END                                           AS is_over_50k_customer_flag, 
  
  CASE 
    WHEN COALESCE(arr.arr,0) > 500000
    THEN 1
    ELSE 0 
  END                                           AS is_over_500k_customer_flag, 
  
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
  COALESCE(net_arr_ttm.ttm_churn_contraction_deal_count,0)  AS ttm_churn_contraction_deal_count,
  COALESCE(net_arr_ttm.ttm_renewal_deal_count,0)            AS ttm_renewal_deal_count,
  
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
  COALESCE(op.count_open_deals,0)                           AS count_open_deals_pipe,
  
  CASE
    WHEN COALESCE(arr.arr,0) > 0
        AND COALESCE(op.open_pipe,0) > 0
            THEN 1
        ELSE 0
  END                                                       AS customer_has_open_pipe_flag,
  
  CASE
    WHEN COALESCE(arr.arr,0) = 0
        AND COALESCE(op.open_pipe,0) > 0
            THEN 1
        ELSE 0
  END                                                       AS prospect_has_open_pipe_flag,
  
  
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
  COALESCE(pg_ly.pg_last_12m_ae_sourced_net_arr,0)          AS pg_last_12m_ae_sourced_net_arr,
  
  
  COALESCE(pg_last_12m_web_direct_sourced_deal_count,0)     AS pg_last_12m_web_direct_sourced_deal_count,
  COALESCE(pg_last_12m_channel_sourced_deal_count,0)        AS pg_last_12m_channel_sourced_deal_count,
  COALESCE(pg_last_12m_sdr_sourced_deal_count,0)            AS pg_last_12m_sdr_sourced_deal_count,
  COALESCE(pg_last_12m_ae_sourced_deal_count,0)             AS pg_last_12m_ae_sourced_deal_count

  
FROM account_year_key ak
  INNER JOIN sfdc_accounts_xf a
    ON ak.account_id = a.account_id
  LEFT JOIN stitch_account raw
    ON ak.account_id = raw.ACCOUNT_ID_18__C
  LEFT JOIN stitch_account uparaw
    ON a.ultimate_parent_account_id = uparaw.ACCOUNT_ID_18__C
  LEFT JOIN country upa_c
    ON uparaw.ACCOUNT_DEMOGRAPHICS_UPA_COUNTRY__C = upa_c.countrycode
  LEFT JOIN sfdc_users_xf u 
    ON a.owner_id = u.user_id
  LEFT JOIN sfdc_accounts_xf upa
    ON a.ultimate_parent_account_id = upa.account_id
  LEFT JOIN sfdc_users_xf upa_owner 
    ON upa.owner_id = upa_owner.user_id
  LEFT JOIN fy_atr_base
    ON a.account_id = fy_atr_base.account_id
    AND fy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN ttm_atr_base ttm_atr
    ON a.account_id = ttm_atr.account_id
    AND ttm_atr.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN nfy_atr_base
    ON a.account_id = nfy_atr_base.account_id
    AND nfy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN net_arr_ttm
    ON a.account_id = net_arr_ttm.account_id
    AND net_arr_ttm.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN op_forward_one_year op
    ON a.account_id = op.account_id
     AND op.report_fiscal_year = ak.report_fiscal_year 
  LEFT JOIN pg_ytd pg
    ON a.account_id = pg.account_id
    AND pg.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_last_12_months pg_ly
    ON a.account_id = pg_ly.account_id
    AND pg_ly.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN arr_at_same_month arr
    ON a.account_id = arr.account_id
    AND arr.report_fiscal_year = ak.report_fiscal_year 
  LEFT JOIN total_net_arr_fiscal net_arr_fiscal
    ON a.account_id = net_arr_fiscal.account_id
    AND net_arr_fiscal.report_fiscal_year = ak.report_fiscal_year 

), consolidated_upa AS (

  SELECT 
    report_fiscal_year,
    upa_id,
    upa_name,
    upa_owner_name,
    upa_owner_id,
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
    MAX(upa_aggregate_dev_count)  AS upa_aggregate_dev_count,

    SUM(has_technical_account_manager_flag) AS count_technical_account_managers,

    -- LAM
   -- MAX(potential_arr_lam)            AS potential_arr_lam,
   -- MAX(potential_carr_this_account)  AS potential_carr_this_account,

    -- atr for current fy
    SUM(fy_sfdc_atr)  AS fy_sfdc_atr,
    -- next fiscal year atr base reported at fy
    SUM(nfy_sfdc_atr) AS nfy_sfdc_atr,

    -- arr by fy
    SUM(arr) AS arr,

    MAX(is_customer_flag)             AS is_customer_flag,
    MAX(is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
    MAX(is_over_10k_customer_flag)    AS is_over_10k_customer_flag,    
    MAX(is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
    MAX(is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
    SUM(is_over_5k_customer_flag)     AS count_over_5k_customers,
    SUM(is_over_10k_customer_flag)    AS count_over_10k_customers,
    SUM(is_over_50k_customer_flag)    AS count_over_50k_customers,
    SUM(is_over_500k_customer_flag)   AS count_over_500k_customers,
    SUM(is_prospect_flag)             AS count_of_prospects,
    SUM(is_customer_flag)             AS count_of_customers,

    SUM(arr_channel)                  AS arr_channel,
    SUM(arr_direct)                   AS arr_direct, 

    SUM(product_starter_arr)          AS product_starter_arr,
    SUM(product_premium_arr)          AS product_premium_arr,
    SUM(product_ultimate_arr)         AS product_ultimate_arr, 
    SUM(delivery_self_managed_arr)    AS delivery_self_managed_arr,
    SUM(delivery_saas_arr)            AS delivery_saas_arr,


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
    SUM(ttm_atr)                      AS ttm_atr,

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
    SUM(count_open_deals_pipe)        AS count_open_deals_pipe,
    SUM(customer_has_open_pipe_flag)  AS customer_has_open_pipe_flag,
    SUM(prospect_has_open_pipe_flag)  AS prospect_has_open_pipe_flag,

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

), upa_lam AS (

    SELECT 
        upa_id,
  
        arr,
  
        CASE
            WHEN potential_users > licenses
                THEN potential_users
             ELSE 0 
        END                                 AS adjusted_potential_users,
  
        CASE
            WHEN linkedin_developer > licenses
                THEN linkedin_developer
             ELSE 0 
        END                                 AS adjusted_linkedin_developers,
  
        CASE
            WHEN zi_developers > licenses
                THEN zi_developers
             ELSE 0 
        END                                 AS adjusted_zi_developers,
  
        CASE
            WHEN employees * 0.1 > licenses
                THEN round(employees * 0.1,0) 
             ELSE 0 
        END                                 AS adjusted_employees,
        
        is_customer_flag,
        
        --LEAST(50000,GREATEST(licenses, COALESCE(adjusted_potential_users,adjusted_linkedin_developers,adjusted_zi_developers,adjusted_employees))) AS lam_dev_count
        upa_aggregate_dev_count  AS lam_dev_count
  
    FROM consolidated_upa
    WHERE report_fiscal_year = 2022


), final AS (

  SELECT
      acc.*,
      CASE 
          WHEN acc.calculated_developer_count > 500 
            THEN 1
          ELSE 0
      END                                       AS account_has_over_500_dev_flag,
      CASE WHEN upa.upa_id = acc.account_id 
           THEN upa.arr ELSE 0 END              AS upa_arr,
  
      CASE WHEN upa.upa_id = acc.account_id 
           THEN upa.adjusted_potential_users 
            ELSE 0 END                          AS upa_potential_users,
      CASE WHEN upa.upa_id = acc.account_id 
           THEN upa.adjusted_linkedin_developers 
            ELSE 0 END                          AS upa_linkedin_developers,
      CASE WHEN upa.upa_id = acc.account_id 
           THEN upa.adjusted_zi_developers 
            ELSE 0 END                          AS upa_zi_developers,
      CASE WHEN upa.upa_id = acc.account_id 
           THEN upa.adjusted_employees 
            ELSE 0 END                          AS upa_employees,
      CASE WHEN upa.upa_id = acc.account_id 
           THEN  COALESCE(upa.lam_dev_count,0)   
            ELSE 0 END                          AS lam_dev_count,
      CASE
        WHEN upa.upa_id = acc.account_id 
            THEN 1
        ELSE 0
       END                                  AS is_upa_flag,
  
        upa.is_customer_flag                AS hierarchy_is_customer_flag,
        CASE 
            WHEN COALESCE(upa.lam_dev_count,0) > 500
                THEN 1
            ELSE 0
        END                                 AS hierarchy_has_over_500_dev_flag
  FROM consolidated_accounts acc
  LEFT JOIN upa_lam upa
      ON upa.upa_id = acc.upa_id
  WHERE acc.report_fiscal_year = 2022
  
)

SELECT *
FROM final