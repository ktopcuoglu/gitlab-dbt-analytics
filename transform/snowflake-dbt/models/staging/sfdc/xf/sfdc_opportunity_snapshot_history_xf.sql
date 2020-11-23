/*

Original issue: 
https://gitlab.com/gitlab-com/sales-team/field-operations/analytics/-/issues/204

2020-09-15
It incorporates flags added to support the pipeline velocity report of @fkurniadi 
and the forecasting model for Commercial of Sales Strategy

-- NF: Is there any difference between net_iacv and net_incremental_acv within this model?
-- NF: do we need to keep this forecast iacv? It seems to be the same as Incremental ACV
*/ 
WITH date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunity_snapshot_history AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history') }}
    WHERE is_deleted = 0

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

), sales_admin_hierarchy AS (
    
    SELECT
      opportunity_id,
      owner_id,
      'CRO'                                                AS level_1,
      CASE 
        WHEN account_owner_team_stamped IN ('APAC', 'EMEA', 'Public Sector','US East', 'US West')
          THEN 'VP Ent'
        WHEN account_owner_team_stamped IN ('Commercial', 'Commercial - SMB', 'SMB', 'SMB - International', 'SMB - US')
          THEN 'VP Comm SMB'
        WHEN account_owner_team_stamped IN ('MM - APAC', 'MM - East', 'MM - EMEA', 'MM - West', 'MM-EMEA','Commercial - MM')
          THEN 'VP Comm MM'
        ELSE NULL
      END                                                  AS level_2,
      CASE account_owner_team_stampeD
        WHEN 'APAC'                 THEN 'RD APAC'
        WHEN 'EMEA'                 THEN 'RD EMEA'
        WHEN 'MM - APAC'            THEN 'ASM - MM - APAC'
        WHEN 'MM - East'            THEN 'ASM - MM - East'
        WHEN 'MM - West'            THEN 'ASM - MM - West'
        WHEN 'MM-EMEA'              THEN 'ASM - MM - EMEA'
        WHEN 'MM - EMEA'            THEN 'ASM - MM - EMEA'
        WHEN 'Public Sector'        THEN 'RD PubSec'
        WHEN 'US East'              THEN 'RD US East'
        WHEN 'US West'              THEN 'RD US West'
        ELSE NULL
      END                                                   AS level_3
    FROM sfdc_opportunity_xf
    WHERE owner_id = '00561000000mpHTAAY' -- sfdc Sales Admin user

), final AS (

    SELECT 
      opp_snapshot.date_actual                                         AS snapshot_date,  
      opp_snapshot.forecast_category_name,                
      opp_snapshot.opportunity_id,
      opp_snapshot.owner_id,    
      opp_snapshot.stage_name,
      opp_snapshot.sales_type,
      opp_snapshot.is_deleted,

      -- metrics
      opp_snapshot.renewal_acv,
      opp_snapshot.incremental_acv,
      opp_snapshot.net_incremental_acv,
      opp_snapshot.total_contract_value,
      opp_snapshot.professional_services_value,
      opp_snapshot.forecasted_iacv,

      -- opportunity driven fields
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.account_owner_team_stamped,     
      
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped IS NULL 
          THEN '3. Growth'
        ELSE sfdc_opportunity_xf.order_type_stamped
      END                                                          AS order_type_stamped,     
     
      --********************************************************
      -- Deprecated field - 2020-10-13
      -- Please use order_type_stamped instead
      
      CASE 
        WHEN sfdc_opportunity_xf.order_type IS NULL 
          THEN '3. Growth'
        ELSE sfdc_opportunity_xf.order_type
      END                                                           AS order_type, 
      --********************************************************    

      -- account driven fields
      sfdc_accounts_xf.tsp_region,
      sfdc_accounts_xf.tsp_sub_region,
      sfdc_accounts_xf.ultimate_parent_sales_segment,

      --date helpers
      snapshot_date.first_day_of_month                              AS snapshot_month,
      snapshot_date.fiscal_year                                     AS snapshot_fiscal_year,
      snapshot_date.fiscal_quarter_name_fy                          AS snapshot_fiscal_quarter,
      snapshot_date.first_day_of_fiscal_quarter                     AS snapshot_fiscal_quarter_date,
      close_date_detail.first_day_of_month                          AS close_month,
      close_date_detail.fiscal_year                                 AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                      AS close_fiscal_quarter,
      close_date_detail.first_day_of_fiscal_quarter                 AS close_fiscal_quarter_date,
      created_date_detail.first_day_of_month                        AS created_month,
      created_date_detail.fiscal_year                               AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                    AS created_fiscal_quarter,
      created_date_detail.first_day_of_fiscal_quarter               AS created_fiscal_quarter_date,

      -- adjusted, as logic is applied to removed as many blanks as possible
      CASE
        WHEN (sfdc_accounts_xf.ultimate_parent_sales_segment  = 'Unknown' 
          OR sfdc_accounts_xf.ultimate_parent_sales_segment  IS NULL) 
          AND sfdc_opportunity_xf.user_segment = 'Mid-Market' 
          THEN 'Mid-Market'
        WHEN (sfdc_accounts_xf.ultimate_parent_sales_segment  = 'Unknown' 
          OR sfdc_accounts_xf.ultimate_parent_sales_segment  IS NULL) 
          AND sfdc_opportunity_xf.user_segment IN ('Large', 'US West', 'US East', 'Public Sector'
                                                    , 'EMEA', 'APAC') 
          THEN 'Large'
        WHEN (sfdc_accounts_xf.ultimate_parent_sales_segment  = 'Unknown' 
              OR sfdc_accounts_xf.ultimate_parent_sales_segment  IS NULL) 
          THEN 'SMB'    
        ELSE sfdc_accounts_xf.ultimate_parent_sales_segment
      END                                                                                                   AS adj_ultimate_parent_sales_segment,
  
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
      END                                                                                                   AS stage_name_3plus,
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
      END                                                                                                   AS stage_name_4plus,
      
      --********************************************************
      -- calculated fields for pipeline velocity report

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      -- excluded accounts 
      CASE 
        WHEN sfdc_accounts_xf.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND opp_snapshot.close_date < '2020-08-01' THEN 1
        ELSE 0
      END                                                                                                   AS is_excluded_flag,

      CASE 
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost') 
          AND opp_snapshot.sales_type = 'Renewal'      
            THEN opp_snapshot.renewal_acv * -1
        WHEN opp_snapshot.stage_name IN ('Closed Won')                                                     
          THEN opp_snapshot.forecasted_iacv  
        ELSE 0
      END                                                                                                   AS net_iacv,
      CASE 
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost') 
          AND opp_snapshot.sales_type = 'Renewal'      
            THEN opp_snapshot.renewal_acv*-1
        WHEN opp_snapshot.stage_name IN ('Closed Won') AND opp_snapshot.forecasted_iacv < 0                           
          THEN opp_snapshot.forecasted_iacv
        ELSE 0
      END                                                                                                   AS churn_only,

      CASE 
        WHEN created_date_detail.fiscal_quarter_name_fy = close_date_detail.fiscal_quarter_name_fy
          AND opp_snapshot.stage_name IN ('Closed Won')  
            THEN opp_snapshot.forecasted_iacv
        ELSE 0
      END                                                                                                   AS created_and_won_iacv,

      -- account owner hierarchies levels
      COALESCE(account_owner.sales_team_level_2,'n/a')                   AS account_owner_team_level_2,
      COALESCE(account_owner.sales_team_level_3,'n/a')                   AS account_owner_team_level_3,
      COALESCE(account_owner.sales_team_level_4,'n/a')                   AS account_owner_team_level_4,
      COALESCE(account_owner.sales_team_vp_level,'n/a')                  AS account_owner_team_vp_level,
      COALESCE(account_owner.sales_team_rd_level,'n/a')                  AS account_owner_team_rd_level,
      COALESCE(account_owner.sales_team_asm_level,'n/a')                 AS account_owner_team_asm_level,
      COALESCE(account_owner.sales_min_hierarchy_level,'n/a')            AS account_owner_min_team_level,
      account_owner.is_lvl_2_vp_flag                                     AS account_owner_is_lvl_2_vp_flag,
      account_owner.sales_region                                         AS account_owner_sales_region,

      -- opportunity owner hierarchies levels
      CASE 
        WHEN sales_admin_hierarchy.level_2 IS NOT NULL 
          THEN sales_admin_hierarchy.level_2 
        ELSE opportunity_owner.sales_team_level_2
      END                                                                AS opportunity_owner_team_level_2,
      CASE 
        WHEN sales_admin_hierarchy.level_3 IS NOT NULL 
          THEN sales_admin_hierarchy.level_3 
        ELSE opportunity_owner.sales_team_level_3
      END                                                                AS opportunity_owner_team_level_3,    
      CASE 
        WHEN LOWER(opportunity_owner.sales_team_level_2) LIKE 'vp%' 
          OR LOWER(sales_admin_hierarchy.level_2) LIKE 'vp%'
            THEN 1 
        ELSE 0
      END                                                                AS opportunity_owner_is_lvl_2_vp_flag

    FROM sfdc_opportunity_snapshot_history opp_snapshot
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = opp_snapshot.close_date
    INNER JOIN date_details snapshot_date
      ON opp_snapshot.date_actual = snapshot_date.date_actual
    LEFT JOIN date_details created_date_detail
      ON created_date_detail.date_actual = opp_snapshot.created_date
    LEFT JOIN sfdc_opportunity_xf    
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN sfdc_accounts_xf
      ON opp_snapshot.account_id = sfdc_accounts_xf.account_id 
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    LEFT JOIN sales_admin_hierarchy
      ON opp_snapshot.opportunity_id = sales_admin_hierarchy.opportunity_id
) 

SELECT *
FROM final
