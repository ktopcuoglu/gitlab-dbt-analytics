{{ config(alias='sfdc_opportunity_snapshot_history_xf') }}

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

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}} 

), sfdc_opportunity_snapshot_history AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_snapshot_history')}}

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}  

), sales_admin_hierarchy AS (
    
    SELECT
      opportunity_id,
      owner_id,
      'CRO'                                              AS level_1,
      CASE account_owner_team_stamped
        WHEN 'APAC'                 THEN 'VP Ent'
        WHEN 'Commercial'           THEN 'VP Comm SMB'
        WHEN 'Commercial - MM'      THEN 'VP Comm MM'
        WHEN 'Commercial - SMB'     THEN 'VP Comm SMB'
        WHEN 'EMEA'                 THEN 'VP Ent'
        WHEN 'MM - APAC'            THEN 'VP Comm MM'
        WHEN 'MM - East'            THEN 'VP Comm MM'
        WHEN 'MM - EMEA'            THEN 'VP Comm MM'
        WHEN 'MM - West'            THEN 'VP Comm MM'
        WHEN 'MM-EMEA'              THEN 'VP Comm MM'
        WHEN 'Public Sector'        THEN 'VP Ent'
        WHEN 'SMB'                  THEN 'VP Comm SMB'
        WHEN 'SMB - International'  THEN 'VP Comm SMB'
        WHEN 'SMB - US'             THEN 'VP Comm SMB'
        WHEN 'US East'              THEN 'VP Ent'
        WHEN 'US West'              THEN 'VP Ent'
        ELSE NULL
      END                                                AS level_2,
      CASE account_owner_team_stamped
        WHEN 'APAC'                 THEN 'RD APAC'
        WHEN 'EMEA'                 THEN 'RD EMEA'
        WHEN 'MM - APAC'            THEN 'ASM - MM - APAC'
        WHEN 'MM - East'            THEN 'ASM - MM - East'
        WHEN 'MM - EMEA'            THEN 'ASM - MM - EMEA'
        WHEN 'MM - West'            THEN 'ASM - MM - West'
        WHEN 'MM-EMEA'              THEN 'ASM - MM - EMEA'
        WHEN 'Public Sector'        THEN 'RD PubSec'
        WHEN 'US East'              THEN 'RD US East'
        WHEN 'US West'              THEN 'RD US West'
        ELSE NULL
      END                                                AS level_3
    FROM sfdc_opportunity_xf
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), net_iacv_to_net_arr_ratio AS (

    SELECT '2. New - Connected'       AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          1.001856868               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.9879780801              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          0.9999751852              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.9983306793              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.9427320642              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.9072734284              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.035889715               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          1                         AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.002887983               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.8504383811              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.9897881218              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          1.012723079               AS "RATIO_NET_IACV_TO_NET_ARR" 

), sfdc_opportunity_snapshot_history_xf AS (

  SELECT DISTINCT
      opp_snapshot.date_actual                                  AS snapshot_date,  
      opp_snapshot.account_id,
      opp_snapshot.forecast_category_name,                
      opp_snapshot.opportunity_id,
      opp_snapshot.owner_id,    
      opp_snapshot.stage_name,
      opp_snapshot.sales_type,
      opp_snapshot.is_deleted,
      opp_snapshot.sales_qualified_source,
      -- metrics
      opp_snapshot.renewal_acv,
      opp_snapshot.incremental_acv,
      opp_snapshot.net_incremental_acv,
      opp_snapshot.total_contract_value,
      opp_snapshot.professional_services_value,

      -- need to apply conversion ratio here
      
      CASE
        WHEN opp_snapshot.net_arr IS NULL 
          AND opp_snapshot.net_incremental_acv <> 0
          THEN opp_snapshot.net_incremental_acv * coalesce(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)
        WHEN opp_snapshot.net_arr IS NULL 
          AND opp_snapshot.incremental_acv <> 0
          THEN opp_snapshot.incremental_acv * coalesce(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)
        ELSE opp_snapshot.net_arr
      END                                                        AS calculated_net_arr,

      opp_snapshot.net_arr,
      opp_snapshot.recurring_amount,
      opp_snapshot.true_up_amount,
      opp_snapshot.proserv_amount,
      opp_snapshot.other_non_recurring_amount,
      opp_snapshot.arr_basis,
      opp_snapshot.arr,
 
      -- opportunity driven fields
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.account_owner_team_stamped, 
      sfdc_opportunity_xf.user_segment_stamped,
           
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped IS NULL 
          THEN '3. Growth'
        ELSE sfdc_opportunity_xf.order_type_stamped
      END                                                        AS order_type_stamped,     
     
      -- account driven fields
      sfdc_accounts_xf.tsp_region,
      sfdc_accounts_xf.tsp_sub_region,
      sfdc_accounts_xf.ultimate_parent_sales_segment,

      --date helpers
      snapshot_date.first_day_of_month                           AS snapshot_date_month,
      snapshot_date.fiscal_year                                  AS snapshot_fiscal_year,
      snapshot_date.fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name,
      snapshot_date.first_day_of_fiscal_quarter                  AS snapshot_fiscal_quarter_date,
      snapshot_date.day_of_fiscal_quarter_normalised             AS snapshot_day_of_fiscal_quarter_normalised,
      
      close_date_detail.first_day_of_month                       AS close_date_month,
      close_date_detail.fiscal_year                              AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                   AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter              AS close_fiscal_quarter_date,

      created_date_detail.first_day_of_month                     AS created_date_month,
      created_date_detail.fiscal_year                            AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                 AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter            AS created_fiscal_quarter_date,

      iacv_created_date.first_day_of_month                       AS iacv_created_date_month,
      iacv_created_date.fiscal_year                              AS iacv_created_fiscal_year,
      iacv_created_date.fiscal_quarter_name_fy                   AS iacv_created_fiscal_quarter_name,
      iacv_created_date.first_day_of_fiscal_quarter              AS iacv_created_fiscal_quarter_date,
       
      CASE 
        WHEN opp_snapshot.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                              ,'Developing', '1-Discovery', '2-Developing', '2-Scoping')  
          THEN 'Pipeline'
        WHEN opp_snapshot.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating'
                              , '6-Awaiting Signature', '7-Closing')                         
          THEN '3+ Pipeline'
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       
          THEN 'Lost'
        WHEN opp_snapshot.stage_name IN ('Closed Won')                                                                                                         
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_3plus,
      
      CASE 
        WHEN opp_snapshot.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                            , 'Developing', '1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
          THEN 'Pipeline'
        WHEN opp_snapshot.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               
          THEN '4+ Pipeline'
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   
          THEN 'Lost'
        WHEN opp_snapshot.stage_name IN ('Closed Won')                                                                                                                                     
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_4plus,

      CASE 
        WHEN opp_snapshot.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_3_plus,

      CASE 
        WHEN opp_snapshot.stage_name 
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_4_plus,


      CASE 
        WHEN opp_snapshot.stage_name = 'Closed Won' 
          THEN 1 ELSE 0
      END                                                         AS is_won,

      CASE 
        WHEN opp_snapshot.stage_name = '8-Closed Lost'  
          THEN 1 ELSE 0
      END                                                         AS is_lost,

      CASE 
        WHEN (opp_snapshot.stage_name = '8-Closed Lost' 
          OR opp_snapshot.stage_name = '9-Unqualified'
          OR opp_snapshot.stage_name = 'Closed Won' ) 
            THEN 0
        ELSE 1  
      END                                                         AS is_open,

      CASE 
        WHEN is_open = 0
          THEN 1
        ELSE 0
      END                                                         AS is_closed,
      
      CASE 
        WHEN opp_snapshot.stage_name = 'Closed Won' THEN '1.Won'
        WHEN is_lost = 1 THEN '2.Lost'
        WHEN is_open = 1 THEN '0. Open' 
        ELSE 'N/A'
      END                                                         AS stage_category,

      CASE 
        WHEN LOWER(opp_snapshot.sales_type) like '%renewal%' 
          THEN 1
        ELSE 0
      END                                                         AS is_renewal, 
      
      -- top level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Churn','4. Contraction','6. Churn - Final') 
          THEN '2. Growth' 
        ELSE '3. Other'
      END                                                         AS deal_group,

      -- medium level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('4. Churn','4. Contraction','6. Churn - Final')
          THEN '3. Churn'
        ELSE '4. Other' 
      END                                                         AS deal_category,

      CASE 
        WHEN created_date_detail.fiscal_quarter_name_fy = close_date_detail.fiscal_quarter_name_fy
          AND opp_snapshot.stage_name IN ('Closed Won')  
            THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                         AS created_and_won_iacv,

      -- created within quarter
      CASE
        WHEN created_date_detail.fiscal_quarter_name_fy = snapshot_date.fiscal_quarter_name_fy
          THEN opp_snapshot.incremental_acv 
        ELSE 0 
      END                                                         AS created_in_quarter_iacv,

      -- account owner hierarchies levels
      COALESCE(account_owner.sales_team_level_2,'n/a')            AS account_owner_team_level_2,
      COALESCE(account_owner.sales_team_level_3,'n/a')            AS account_owner_team_level_3,
      COALESCE(account_owner.sales_team_level_4,'n/a')            AS account_owner_team_level_4,
      COALESCE(account_owner.sales_team_vp_level,'n/a')           AS account_owner_team_vp_level,
      COALESCE(account_owner.sales_team_rd_level,'n/a')           AS account_owner_team_rd_level,
      COALESCE(account_owner.sales_team_asm_level,'n/a')          AS account_owner_team_asm_level,
      COALESCE(account_owner.sales_min_hierarchy_level,'n/a')     AS account_owner_min_team_level,
      account_owner.sales_region                                  AS account_owner_sales_region,
  
      CASE 
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Ent'
            THEN 'Large'
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Comm MM'
            THEN 'Mid-Market'
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Comm SMB' 
            THEN 'SMB' 
          ELSE 'Other' 
      END                                                         AS account_owner_cro_level,
  
      -- opportunity owner hierarchies levels
      CASE 
        WHEN sales_admin_hierarchy.level_2 IS NOT NULL 
          THEN sales_admin_hierarchy.level_2 
        ELSE opportunity_owner.sales_team_level_2
      END                                                         AS opportunity_owner_team_level_2,
      
      CASE 
        WHEN sales_admin_hierarchy.level_3 IS NOT NULL 
          THEN sales_admin_hierarchy.level_3 
        ELSE opportunity_owner.sales_team_level_3
      END                                                         AS opportunity_owner_team_level_3,    

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN sfdc_accounts_xf.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND opp_snapshot.close_date < '2020-08-01' 
            THEN 1
        ELSE 0
      END                                                         AS is_excluded_flag
      

    FROM sfdc_opportunity_snapshot_history opp_snapshot
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = opp_snapshot.close_date::date
    INNER JOIN date_details snapshot_date
      ON opp_snapshot.date_actual::date = snapshot_date.date_actual
    INNER JOIN sfdc_opportunity_xf    
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN date_details created_date_detail
      ON created_date_detail.date_actual = opp_snapshot.created_date::date
    LEFT JOIN date_details iacv_created_date
      ON iacv_created_date.date_actual = opp_snapshot.iacv_created_date::DATE
    LEFT JOIN sfdc_accounts_xf
      ON opp_snapshot.account_id = sfdc_accounts_xf.account_id 
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    LEFT JOIN sales_admin_hierarchy
      ON opp_snapshot.opportunity_id = sales_admin_hierarchy.opportunity_id
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = sfdc_opportunity_xf.user_segment_stamped
      AND net_iacv_to_net_arr_ratio.order_type_stamped = sfdc_opportunity_xf.order_type_stamped
     -- remove test account
    WHERE opp_snapshot.account_id not in ('0014M00001kGcORQA0')
      AND opp_snapshot.is_deleted = 0
)

SELECT *
FROM sfdc_opportunity_snapshot_history_xf
  
 
