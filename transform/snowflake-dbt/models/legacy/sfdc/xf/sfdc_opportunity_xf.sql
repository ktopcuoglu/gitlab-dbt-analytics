WITH sfdc_opportunity AS (

    SELECT * FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunity_stage AS (

    SELECT * FROM {{ref('sfdc_opportunity_stage')}}

), sfdc_lead_source AS (

    SELECT * FROM {{ref('sfdc_lead_sources')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), sfdc_account AS (

    SELECT * FROM {{ref('sfdc_account')}}

), date_details AS (
 
    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sales_admin_hierarchy AS (
    
    SELECT
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.owner_id,
      'CRO'                                                           AS level_1,
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
      END                                                             AS level_2,
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
      END                                                             AS level_3
    FROM sfdc_opportunity
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), layered AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id,
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.opportunity_name,
      sfdc_opportunity.owner_id,

      -- logistical information
      sfdc_opportunity.close_date,
      sfdc_opportunity.created_date,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.deployment_preference,
      sfdc_opportunity.generated_source,
      sfdc_opportunity.lead_source,
      sfdc_lead_source.lead_source_id                                                             AS lead_source_id,
      COALESCE(sfdc_lead_source.initial_source, 'Unknown')                                        AS lead_source_name,
      COALESCE(sfdc_lead_source.initial_source_type, 'Unknown')                                   AS lead_source_type,
      sfdc_opportunity.merged_opportunity_id,
      sfdc_opportunity.net_new_source_categories,
      sfdc_opportunity.opportunity_business_development_representative,
      sfdc_opportunity.opportunity_owner                                                          AS opportunity_owner,
      sfdc_opportunity.opportunity_owner_department                                               AS opportunity_owner_department,
      sfdc_opportunity.opportunity_owner_manager                                                  AS opportunity_owner_manager,
      opportunity_owner.role_name                                                                 AS opportunity_owner_role,
      sfdc_opportunity.opportunity_owner_team                                                     AS opportunity_owner_team,
      opportunity_owner.title                                                                     AS opportunity_owner_title,
      sfdc_opportunity.opportunity_sales_development_representative,
      sfdc_opportunity.opportunity_development_representative,
      sfdc_opportunity.account_owner_team_stamped,
      sfdc_opportunity.opportunity_term,
      sfdc_opportunity.primary_campaign_source_id                                                 AS primary_campaign_source_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_path,
      sfdc_opportunity.sales_qualified_date,
      sfdc_opportunity.sales_type,
      sfdc_opportunity.sdr_pipeline_contribution,
      sfdc_opportunity.source_buckets,
      sfdc_opportunity.stage_name,
      sfdc_opportunity_stage.is_active                                                             AS stage_is_active,
      sfdc_opportunity_stage.is_closed                                                             AS stage_is_closed,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.order_type,
      sfdc_opportunity.deal_path,

      -- opportunity information
      sfdc_opportunity.acv,
      sfdc_opportunity.amount,
      sfdc_opportunity.closed_deals,
      sfdc_opportunity.competitors,
      sfdc_opportunity.critical_deal_flag,
      sfdc_opportunity.deal_size,
      sfdc_opportunity.forecast_category_name,
      sfdc_opportunity.forecasted_iacv,
      sfdc_opportunity.iacv_created_date,
      sfdc_opportunity.incremental_acv,
      sfdc_opportunity.pre_covid_iacv,
      sfdc_opportunity.invoice_number,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      CASE 
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_risky,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      sfdc_opportunity.net_incremental_acv,
      sfdc_opportunity.probability,
      sfdc_opportunity.professional_services_value,
      sfdc_opportunity.pushed_count,
      sfdc_opportunity.reason_for_loss,
      sfdc_opportunity.reason_for_loss_details,
      sfdc_opportunity.refund_iacv,
      sfdc_opportunity.downgrade_iacv,
      sfdc_opportunity.renewal_acv,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.sales_qualified_source,
      sfdc_opportunity.solutions_to_be_replaced,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.upside_iacv,
      sfdc_opportunity.upside_swing_deal_iacv,
      sfdc_opportunity.incremental_acv * (probability /100)                                       AS weighted_iacv,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.partner_initiated_opportunity,
      sfdc_opportunity.user_segment,
      sfdc_opportunity.subscription_start_date,
      sfdc_opportunity.subscription_end_date,
      sfdc_opportunity.true_up_value,
      sfdc_opportunity.order_type_live,
      sfdc_opportunity.order_type_stamped,
      sfdc_opportunity.net_arr,

      -- days and dates per stage
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.stage_0_pending_acceptance_date,
      sfdc_opportunity.stage_1_discovery_date,
      sfdc_opportunity.stage_2_scoping_date,
      sfdc_opportunity.stage_3_technical_evaluation_date,
      sfdc_opportunity.stage_4_proposal_date,
      sfdc_opportunity.stage_5_negotiating_date,
      sfdc_opportunity.stage_6_awaiting_signature_date,
      sfdc_opportunity.stage_6_closed_won_date,
      sfdc_opportunity.stage_6_closed_lost_date,

      -- command plan fields
      sfdc_opportunity.cp_champion,
      sfdc_opportunity.cp_close_plan,
      sfdc_opportunity.cp_competition,
      sfdc_opportunity.cp_decision_criteria,
      sfdc_opportunity.cp_decision_process,
      sfdc_opportunity.cp_economic_buyer,
      sfdc_opportunity.cp_identify_pain,
      sfdc_opportunity.cp_metrics,
      sfdc_opportunity.cp_risks,
      sfdc_opportunity.cp_use_cases,
      sfdc_opportunity.cp_value_driver,
      sfdc_opportunity.cp_why_do_anything_at_all,
      sfdc_opportunity.cp_why_gitlab,
      sfdc_opportunity.cp_why_now,

      -- sales segment refactor
      sfdc_opportunity.division_sales_segment_stamped,
      {{ sales_segment_cleaning('sfdc_account.tsp_max_hierarchy_sales_segment') }}                        AS tsp_max_hierarchy_sales_segment,
      sfdc_account.division_sales_segment,
      sfdc_account.ultimate_parent_sales_segment,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
      sfdc_opportunity.segment                                                                          AS segment,
      sfdc_opportunity.sales_segment                                                                    AS sales_segment,
      sfdc_opportunity.parent_segment                                                                   AS parent_segment,
      
      -- ************************************
      -- channel reporting
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
      
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,

      CASE
        WHEN sfdc_opportunity.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity.order_type_stamped = '4. Churn'
          THEN '3. Churn'
        ELSE '4. Other'
      END                                                                                               AS deal_category,
    
      -- adjusted, as logic is applied to removed as many blanks as possible
      CASE
        WHEN (sfdc_account.ultimate_parent_sales_segment  = 'Unknown' OR sfdc_account.ultimate_parent_sales_segment  IS NULL) 
          AND sfdc_opportunity.user_segment = 'Mid-Market' 
            THEN 'Mid-Market'
        WHEN (sfdc_account.ultimate_parent_sales_segment  = 'Unknown' OR sfdc_account.ultimate_parent_sales_segment  IS NULL) 
          AND sfdc_opportunity.user_segment IN ('Large', 'US West', 'US East', 'Public Sector', 'EMEA', 'APAC') 
            THEN 'Large'
        WHEN (sfdc_account.ultimate_parent_sales_segment  = 'Unknown' OR sfdc_account.ultimate_parent_sales_segment  IS NULL) 
            THEN 'SMB'    
        ELSE sfdc_account.ultimate_parent_sales_segment
      END                                                                                                 AS adj_ultimate_parent_sales_segment,
     
      -- account owner hierarchies levels
      account_owner.sales_team_level_2                                                                    AS account_owner_team_level_2,
      account_owner.sales_team_level_3                                                                    AS account_owner_team_level_3,
      account_owner.sales_team_level_4                                                                    AS account_owner_team_level_4,       
      account_owner.sales_team_vp_level                                                                   AS account_owner_team_vp_level,
      account_owner.sales_team_rd_level                                                                   AS account_owner_team_rd_level,
      account_owner.sales_team_asm_level                                                                  AS account_owner_team_asm_level,
      account_owner.sales_min_hierarchy_level                                                             AS account_owner_min_team_level,
      account_owner.sales_region                                                                          AS account_owner_sales_region,
      account_owner.is_lvl_2_vp_flag                                                                      AS account_owner_is_lvl_2_vp_flag,

      -- opportunity owner hierarchies levels
      CASE
        WHEN sales_admin_hierarchy.level_2 IS NOT NULL 
          THEN sales_admin_hierarchy.level_2 
        ELSE opportunity_owner.sales_team_level_2
      END                                                                                                AS opportunity_owner_team_level_2,
      CASE 
        WHEN sales_admin_hierarchy.level_3 IS NOT NULL 
          THEN sales_admin_hierarchy.level_3 
        ELSE opportunity_owner.sales_team_level_3
      END                                                                                                AS opportunity_owner_team_level_3,
      CASE 
        WHEN opportunity_owner.sales_team_level_2 LIKE 'VP%' 
          OR sales_admin_hierarchy.level_2 LIKE 'VP%'
            THEN 1 
        ELSE 0
      END                                                                                                AS opportunity_owner_is_lvl_2_vp_flag,

      -- reporting helper flags
      CASE 
        WHEN sfdc_opportunity.stage_name 
          IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  
            THEN 'Pipeline'													
        WHEN sfdc_opportunity.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN '3+ Pipeline'													
        WHEN sfdc_opportunity.stage_name 
          IN ('8-Closed Lost', 'Closed Lost')                                                                                             
            THEN 'Lost'													  
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')                                                                                             
            THEN 'Closed Won'													
        ELSE 'Other'
      END                                                                                                AS stage_name_3plus,												
      
      CASE 
        WHEN sfdc_opportunity.stage_name 
          IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
            THEN 'Pipeline'													
        WHEN sfdc_opportunity.stage_name 
          IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                           
            THEN '4+ Pipeline'													
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                             
            THEN 'Lost'													
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')                                                                                               
            THEN 'Closed Won'													
        ELSE 'Other'
      END                                                                                                AS stage_name_4plus,	

      CASE 
        WHEN sfdc_opportunity.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                                                               AS is_stage_3_plus,

      CASE 
        WHEN sfdc_opportunity.stage_name = '8-Closed Lost'  
          THEN 1 ELSE 0
      END                                                                                               AS is_lost,
      CASE 
        WHEN (sfdc_opportunity.stage_name = '8-Closed Lost' 
          OR sfdc_opportunity.stage_name = '9-Unqualified'
          OR sfdc_opportunity_stage.is_won = 1) 
            THEN 0
        ELSE 1  
      END                                                                                               AS is_open,

      CASE 
        WHEN is_open = 0
          THEN 1
        ELSE 0
      END                                                                                               AS is_closed,
      
      CASE 
        WHEN sfdc_opportunity_stage.is_won = 1 THEN '1.Won'
        WHEN is_lost = 1 THEN '2.Lost'
        WHEN is_open = 1 THEN '0. Open' 
        ELSE 'N/A'
      END                                                                                               AS stage_category,

      CASE 
        WHEN LOWER(sfdc_opportunity.sales_type) like '%renewal%' 
          THEN 1
        ELSE 0
      END                                                                                               AS is_renewal, 

      -- date fields helpers
      close_date_detail.fiscal_quarter_name_fy                                                          AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter                                                     AS close_fiscal_quarter_date,
      close_date_detail.fiscal_year                                                                     AS close_fiscal_year,
      close_date_detail.first_day_of_month                                                              AS close_date_month,
      
      created_date_detail.fiscal_quarter_name_fy                                                        AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter                                                   AS created_fiscal_quarter_date,
      created_date_detail.fiscal_year                                                                   AS created_fiscal_year,
      created_date_detail.first_day_of_month                                                            AS created_date_month,

      start_date.fiscal_year                                                                            AS subscription_start_date_fiscal_year,
      start_date.fiscal_quarter_name_fy                                                                 AS subscription_start_date_fiscal_quarter_name,
      start_date.first_day_of_month                                                                     AS subscription_start_date_month,
     
      sales_accepted_date.fiscal_quarter_name_fy                                                        AS sales_accepted_fiscal_quarter_name,
      sales_accepted_date.fiscal_year                                                                   AS sales_accepted_fiscal_year,
      sales_accepted_date.first_day_of_month                                                            AS sales_accepted_date_month,

      sales_qualified_date.fiscal_quarter_name_fy                                                       AS sales_qualified_fiscal_quarter_name,
      sales_qualified_date.fiscal_year                                                                  AS sales_qualified_fiscal_year,
      sales_qualified_date.first_day_of_month                                                           AS sales_qualified_date_month,      

      -- metadata
      sfdc_opportunity._last_dbt_run,
      sfdc_record_type.business_process_id,
      sfdc_opportunity.days_since_last_activity,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.last_activity_date,
      sfdc_record_type.record_type_description,
      sfdc_opportunity.record_type_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_record_type.record_type_name,
      MD5((DATE_TRUNC('month', sfdc_opportunity.close_date)::DATE)||UPPER(opportunity_owner.team))      AS region_quota_id,
      MD5((DATE_TRUNC('month', sfdc_opportunity.close_date)::DATE)||UPPER(opportunity_owner.name))      AS sales_quota_id,

      --********************************************************
      -- calculated fields for pipeline velocity report
      
      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN sfdc_account.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND sfdc_opportunity.close_date < '2020-08-01' 
            THEN 1
        ELSE 0
      END                                                                                               AS is_excluded_flag

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = sfdc_opportunity.close_date::DATE
    INNER JOIN date_details created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity.created_date::DATE
    LEFT JOIN sfdc_lead_source
      ON sfdc_opportunity.lead_source = sfdc_lead_source.initial_source
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON sfdc_opportunity.owner_id = opportunity_owner.user_id
    LEFT JOIN sfdc_record_type
      ON sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN sfdc_account
      ON sfdc_account.account_id = sfdc_opportunity.account_id
    LEFT JOIN date_details sales_accepted_date
      ON sfdc_opportunity.sales_accepted_date::DATE = sales_accepted_date.date_actual
    LEFT JOIN date_details start_date
      ON sfdc_opportunity.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN date_details sales_qualified_date
      ON sfdc_opportunity.sales_qualified_date::DATE = sales_qualified_date.date_actual
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_account.owner_id
    LEFT JOIN sales_admin_hierarchy 
      ON sfdc_opportunity.opportunity_id = sales_admin_hierarchy.opportunity_id
)

SELECT *
FROM layered
