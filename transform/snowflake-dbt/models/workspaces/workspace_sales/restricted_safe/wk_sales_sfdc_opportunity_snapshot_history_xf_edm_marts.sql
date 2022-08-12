{{ config(alias='sfdc_opportunity_snapshot_history_xf_edm_marts') }}
-- TODO
-- Add CS churn fields into model from wk_sales_opportunity object

WITH sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}} 

), sfdc_opportunity_snapshot_history_legacy AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_snapshot_history')}}
    -- FROM prod.restricted_safe_legacy.sfdc_opportunity_snapshot_history

), sfdc_opportunity_xf AS (

    SELECT 
      opportunity_id,
      owner_id,
      account_id,
      order_type_stamped,
      deal_category,
      opportunity_category,
      deal_group,
      opportunity_owner_manager,
      is_edu_oss,
      account_owner_team_stamped, 

      -- Opportunity Owner Stamped fields
      opportunity_owner_user_segment,
      opportunity_owner_user_region,
      opportunity_owner_user_area,
      opportunity_owner_user_geo,

      -------------------
      --  NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in Sisense are updated
      sales_team_rd_asm_level,
      -------------------

      sales_team_cro_level,
      sales_team_vp_level,
      sales_team_avp_rd_level,
      sales_team_asm_level,

      -- this fields use the opportunity owner version for current FY and account fields for previous years
      report_opportunity_user_segment,
      report_opportunity_user_geo,
      report_opportunity_user_region,
      report_opportunity_user_area,
      report_user_segment_geo_region_area,
      report_user_segment_geo_region_area_sqs_ot,

      -- NF 2022-02-17 new aggregated keys 
      key_sqs,
      key_ot,

      key_segment,
      key_segment_sqs,                 
      key_segment_ot,    

      key_segment_geo,
      key_segment_geo_sqs,
      key_segment_geo_ot,      

      key_segment_geo_region,
      key_segment_geo_region_sqs,
      key_segment_geo_region_ot,   

      key_segment_geo_region_area,
      key_segment_geo_region_area_sqs,
      key_segment_geo_region_area_ot,

      key_segment_geo_area,
      
      -------------------------------------
      -- NF: These fields are not exposed yet in opty history, just for check
      -- I am adding this logic

      stage_1_date,
      stage_1_date_month,
      stage_1_fiscal_year,
      stage_1_fiscal_quarter_name,
      stage_1_fiscal_quarter_date,
      --------------------------------------

      is_won,
      is_duplicate_flag,
      raw_net_arr,
      net_incremental_acv,
      sales_qualified_source,
      incremental_acv

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      --deal_path,
      --dr_partner_deal_type,
      --dr_partner_engagement,
      --partner_account,
      --dr_status,
      --distributor,
      --influence_partner,
      --fulfillment_partner,
      --platform_partner,
      --partner_track,
      --is_public_sector_opp,
      --is_registration_from_portal,
      --calculated_discount,
      --partner_discount,
      --partner_discount_calc,
      --comp_channel_neutral

    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('wk_sales_sfdc_users_xf')}}  


-- all the fields are sourcing from edm opp snapshot
), sfdc_opportunity_snapshot_history AS (
    SELECT 
      edm_snapshot_opty.crm_opportunity_snapshot_id AS opportunity_snapshot_id,
      edm_snapshot_opty.dim_crm_opportunity_id AS opportunity_id,
      edm_snapshot_opty.opportunity_name,
      edm_snapshot_opty.owner_id,
      edm_snapshot_opty.opportunity_owner_department,

      edm_snapshot_opty.close_date,
      edm_snapshot_opty.created_date,
      edm_snapshot_opty.sales_qualified_date,
      edm_snapshot_opty.sales_accepted_date,

      edm_snapshot_opty.opportunity_sales_development_representative,
      edm_snapshot_opty.opportunity_business_development_representative,
      edm_snapshot_opty.opportunity_development_representative,

      sfdc_opportunity_snapshot_history.order_type_stamped          AS snapshot_order_type_stamped,
      edm_snapshot_opty.sales_qualified_source_name AS snapshot_sales_qualified_source,
      edm_snapshot_opty.is_edu_oss AS  snapshot_is_edu_oss,
      edm_snapshot_opty.opportunity_category                        AS snapshot_opportunity_category,

      -- Accounts might get deleted or merged, I am selecting the latest account id from the opty object
      -- to avoid showing non-valid account ids
      edm_snapshot_opty.dim_crm_account_id   AS raw_account_id,
      edm_snapshot_opty.raw_net_arr,
      --sfdc_opportunity_snapshot_history.incremental_acv,
      --sfdc_opportunity_snapshot_history.net_incremental_acv,

      edm_snapshot_opty.deployment_preference,
      edm_snapshot_opty.merged_opportunity_id,
      edm_snapshot_opty.sales_path,
      edm_snapshot_opty.sales_type,
      edm_snapshot_opty.stage_name,
      edm_snapshot_opty.competitors,
      edm_snapshot_opty.forecast_category_name,
      edm_snapshot_opty.invoice_number,
      edm_snapshot_opty.primary_campaign_source_id,
      edm_snapshot_opty.professional_services_value,
      edm_snapshot_opty.total_contract_value,
      edm_snapshot_opty.is_web_portal_purchase,
      edm_snapshot_opty.opportunity_term,
      edm_snapshot_opty.arr_basis,
      edm_snapshot_opty.arr,
      edm_snapshot_opty.amount,
      edm_snapshot_opty.recurring_amount,
      edm_snapshot_opty.true_up_amount,
      edm_snapshot_opty.proserv_amount,
      edm_snapshot_opty.renewal_amount,
      edm_snapshot_opty.other_non_recurring_amount,
      edm_snapshot_opty.subscription_start_date AS quote_start_date,
      edm_snapshot_opty.subscription_end_date AS quote_end_date,
      
      edm_snapshot_opty.cp_champion,
      edm_snapshot_opty.cp_close_plan,
      edm_snapshot_opty.cp_competition,
      edm_snapshot_opty.cp_decision_criteria,
      edm_snapshot_opty.cp_decision_process,
      edm_snapshot_opty.cp_economic_buyer,
      edm_snapshot_opty.cp_identify_pain,
      edm_snapshot_opty.cp_metrics,
      edm_snapshot_opty.cp_risks,
      edm_snapshot_opty.cp_use_cases,
      edm_snapshot_opty.cp_value_driver,
      edm_snapshot_opty.cp_why_do_anything_at_all,
      edm_snapshot_opty.cp_why_gitlab,
      edm_snapshot_opty.cp_why_now,
      edm_snapshot_opty.cp_score,

      edm_snapshot_opty.dbt_updated_at AS _last_dbt_run,
      edm_snapshot_opty.is_deleted,
      edm_snapshot_opty.last_activity_date,

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      edm_snapshot_opty.deal_path_name AS deal_path,
      edm_snapshot_opty.dr_partner_deal_type,
      edm_snapshot_opty.dr_partner_engagement,
      edm_snapshot_opty.partner_account,
      edm_snapshot_opty.dr_status,
      edm_snapshot_opty.distributor,
      edm_snapshot_opty.influence_partner,
      edm_snapshot_opty.fulfillment_partner,
      edm_snapshot_opty.platform_partner,
      edm_snapshot_opty.partner_track,
      edm_snapshot_opty.is_public_sector_opp,
      edm_snapshot_opty.is_registration_from_portal,
      edm_snapshot_opty.calculated_discount,
      edm_snapshot_opty.partner_discount,
      edm_snapshot_opty.partner_discount_calc,
      edm_snapshot_opty.comp_channel_neutral,
      edm_snapshot_opty.fpa_master_bookings_flag,
      
      -- stage dates
      -- dates in stage fields
      edm_snapshot_opty.stage_0_pending_acceptance_date,
      edm_snapshot_opty.stage_1_discovery_date,
      edm_snapshot_opty.stage_2_scoping_date,
      edm_snapshot_opty.stage_3_technical_evaluation_date,
      edm_snapshot_opty.stage_4_proposal_date,
      edm_snapshot_opty.stage_5_negotiating_date,
      sfdc_opportunity_snapshot_history.stage_6_awaiting_signature_date,
      edm_snapshot_opty.stage_6_closed_won_date,
      edm_snapshot_opty.stage_6_closed_lost_date,
      
      edm_snapshot_opty.deal_path_engagement,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      edm_snapshot_opty.stage_name_3plus,
      edm_snapshot_opty.stage_name_4plus,
      edm_snapshot_opty.is_stage_1_plus,
      edm_snapshot_opty.is_stage_3_plus,
      edm_snapshot_opty.is_stage_4_plus,
      edm_snapshot_opty.is_won,
      edm_snapshot_opty.is_lost,
      edm_snapshot_opty.is_open,
      edm_snapshot_opty.is_closed,
      edm_snapshot_opty.is_renewal,

      edm_snapshot_opty.is_credit AS is_credit_flag,
      edm_snapshot_opty.is_refund,
      edm_snapshot_opty.is_contract_reset AS is_contract_reset_flag,

      -- NF: 20210827 Fields for competitor analysis
      edm_snapshot_opty.competitors_other_flag,
      edm_snapshot_opty.competitors_gitlab_core_flag,
      edm_snapshot_opty.competitors_none_flag,
      edm_snapshot_opty.competitors_github_enterprise_flag,
      edm_snapshot_opty.competitors_bitbucket_server_flag,
      edm_snapshot_opty.competitors_unknown_flag,
      edm_snapshot_opty.competitors_github_flag,
      edm_snapshot_opty.competitors_gitlab_flag,
      edm_snapshot_opty.competitors_jenkins_flag,
      edm_snapshot_opty.competitors_azure_devops_flag,
      edm_snapshot_opty.competitors_svn_flag,
      edm_snapshot_opty.competitors_bitbucket_flag,
      edm_snapshot_opty.competitors_atlassian_flag,
      edm_snapshot_opty.competitors_perforce_flag,
      edm_snapshot_opty.competitors_visual_studio_flag,
      edm_snapshot_opty.competitors_azure_flag,
      edm_snapshot_opty.competitors_amazon_code_commit_flag,
      edm_snapshot_opty.competitors_circleci_flag,
      edm_snapshot_opty.competitors_bamboo_flag,
      edm_snapshot_opty.competitors_aws_flag,

      edm_snapshot_opty.stage_category,
      edm_snapshot_opty.pipeline_calculated_deal_count AS calculated_deal_count,
      -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      edm_snapshot_opty.calculated_age_in_days,

      --date helpers
      edm_snapshot_opty.snapshot_date,
      edm_snapshot_opty.snapshot_month AS snapshot_date_month,
      edm_snapshot_opty.snapshot_fiscal_year,
      edm_snapshot_opty.snapshot_fiscal_quarter_name,
      edm_snapshot_opty.snapshot_fiscal_quarter_date,
      edm_snapshot_opty.snapshot_day_of_fiscal_quarter_normalised,
      edm_snapshot_opty.snapshot_day_of_fiscal_year_normalised,

      edm_snapshot_opty.close_month  AS close_date_month,
      edm_snapshot_opty.close_fiscal_year,
      edm_snapshot_opty.close_fiscal_quarter_name,
      edm_snapshot_opty.close_fiscal_quarter_date,

      -- This refers to the closing quarter perspective instead of the snapshot quarter
      edm_snapshot_opty.close_day_of_fiscal_quarter_normalised,

      edm_snapshot_opty.created_month AS created_date_month,
      edm_snapshot_opty.created_fiscal_year,
      edm_snapshot_opty.created_fiscal_quarter_name,
      edm_snapshot_opty.created_fiscal_quarter_date,

      edm_snapshot_opty.net_arr_created_date,
      edm_snapshot_opty.net_arr_created_month AS net_arr_created_date_month,
      edm_snapshot_opty.net_arr_created_fiscal_year,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_name,
      edm_snapshot_opty.net_arr_created_fiscal_quarter_date,

      edm_snapshot_opty.pipeline_created_date,
      edm_snapshot_opty.pipeline_created_month AS pipeline_created_date_month,
      edm_snapshot_opty.pipeline_created_fiscal_year,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_name,
      edm_snapshot_opty.pipeline_created_fiscal_quarter_date,

      edm_snapshot_opty.sales_accepted_month,
      edm_snapshot_opty.sales_accepted_fiscal_year,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_name,
      edm_snapshot_opty.sales_accepted_fiscal_quarter_date,

    --------------------------------------------

      edm_snapshot_opty.lead_source,
      edm_snapshot_opty.net_new_source_categories,           
      edm_snapshot_opty.record_type_id,
      
      edm_snapshot_opty.deal_size,
      edm_snapshot_opty.calculated_deal_size,
      edm_snapshot_opty.is_eligible_open_pipeline AS is_eligible_open_pipeline_flag,
      edm_snapshot_opty.is_eligible_created_pipeline_flag,
      edm_snapshot_opty.is_eligible_sao_flag,
      edm_snapshot_opty.is_eligible_asp_analysis AS is_eligible_asp_analysis_flag,
      edm_snapshot_opty.is_eligible_age_analysis AS is_eligible_age_analysis_flag,
      edm_snapshot_opty.is_booked_net_arr AS is_booked_net_arr_flag,
      edm_snapshot_opty.is_eligible_churn_contraction AS is_eligible_churn_contraction_flag,
      edm_snapshot_opty.created_in_snapshot_quarter_net_arr,
      edm_snapshot_opty.created_and_won_same_quarter_net_arr,
      edm_snapshot_opty.created_in_snapshot_quarter_deal_count,
      edm_snapshot_opty.open_1plus_deal_count,
      edm_snapshot_opty.open_3plus_deal_count,
      edm_snapshot_opty.open_4plus_deal_count,
      edm_snapshot_opty.booked_deal_count,
      edm_snapshot_opty.churned_contraction_deal_count,
      edm_snapshot_opty.open_1plus_net_arr,
      edm_snapshot_opty.open_3plus_net_arr,
      edm_snapshot_opty.open_4plus_net_arr,
      edm_snapshot_opty.booked_net_arr,
      edm_snapshot_opty.churned_contraction_net_arr,
      edm_snapshot_opty.is_excluded_from_pipeline_created AS is_excluded_flag


    FROM {{ref('mart_crm_opportunity_daily_snapshot')}} AS edm_snapshot_opty
    LEFT JOIN sfdc_opportunity_snapshot_history_legacy AS sfdc_opportunity_snapshot_history
      ON edm_snapshot_opty.opportunity_id = sfdc_opportunity_snapshot_history.opportunity_id
      AND edm_snapshot_opty.snapshot_date = sfdc_opportunity_snapshot_history.date_actual::DATE

    -- INNER JOIN date_details close_date_detail
    --   ON close_date_detail.date_actual = sfdc_opportunity_snapshot_history.close_date::DATE
    -- INNER JOIN date_details snapshot_date
    --   ON sfdc_opportunity_snapshot_history.date_actual::DATE = snapshot_date.date_actual
    -- LEFT JOIN date_details created_date_detail
    --   ON created_date_detail.date_actual = sfdc_opportunity_snapshot_history.created_date::DATE
    -- LEFT JOIN date_details net_arr_created_date
    --   ON net_arr_created_date.date_actual = sfdc_opportunity_snapshot_history.iacv_created_date::DATE
    -- LEFT JOIN date_details sales_accepted_date
    --   ON sales_accepted_date.date_actual = sfdc_opportunity_snapshot_history.sales_accepted_date::DATE


), sfdc_opportunity_snapshot_history_xf AS (

  SELECT DISTINCT
      opp_snapshot.*,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- opportunity driven fields
      
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.is_edu_oss,
      sfdc_opportunity_xf.sales_qualified_source,
      sfdc_opportunity_xf.account_id,
      sfdc_opportunity_xf.opportunity_category,

      
      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date 
        ELSE NULL
      END                                               AS stage_1_date,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_date_month 
        ELSE NULL
      END                                               AS stage_1_date_month,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_year 
        ELSE NULL
      END                                               AS stage_1_fiscal_year,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_name 
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_name,

      CASE 
        WHEN sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
          THEN sfdc_opportunity_xf.stage_1_fiscal_quarter_date 
        ELSE NULL
      END                                               AS stage_1_fiscal_quarter_date,


      -- field used for FY21 bookings reporitng
      sfdc_opportunity_xf.account_owner_team_stamped, 
     
      -- temporary, to deal with global reports that use account_owner_team_stamp field
      CASE 
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('Commercial - SMB','SMB','SMB - US','SMB - International')
          THEN 'SMB'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('APAC','EMEA','Channel','US West','US East','Public Sector')
          THEN 'Large'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('MM - APAC','MM - East','MM - EMEA','Commercial - MM','MM - West','MM-EMEA')
          THEN 'Mid-Market'
        ELSE 'SMB'
      END                                                         AS account_owner_team_stamped_cro_level,   

      -- Team Segment / ASM - RD 
      -- As the snapshot history table is used to compare current perspective with the past, I leverage the most recent version
      -- of the truth ato cut the data, that's why instead of using the stampped version, I take the current fields.
      -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields

      /*

      FY23 - NF 2022-01-28 

      At this point I still think the best is to keep taking the owner / account demographics cuts from the most recent version of the opportunity object.

      The snapshot history at this point is mainly used to track how current performance compares with previous quarters and years
      and to do that effectively the patches / territories must be the same. Any error that is corrected in the future should be incorporated 
      into the overview

      */

      sfdc_opportunity_xf.opportunity_owner_user_segment,
      sfdc_opportunity_xf.opportunity_owner_user_region,
      sfdc_opportunity_xf.opportunity_owner_user_area,
      sfdc_opportunity_xf.opportunity_owner_user_geo,

      --- target fields for reporting, changing their name might help to isolate their logic from the actual field
      -------------------
      --  NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in Sisense are updated
      sfdc_opportunity_xf.sales_team_rd_asm_level,
      -------------------

      sfdc_opportunity_xf.sales_team_cro_level,
      sfdc_opportunity_xf.sales_team_vp_level,
      sfdc_opportunity_xf.sales_team_avp_rd_level,
      sfdc_opportunity_xf.sales_team_asm_level,

      -- this fields use the opportunity owner version for current FY and account fields for previous years
      sfdc_opportunity_xf.report_opportunity_user_segment,
      sfdc_opportunity_xf.report_opportunity_user_geo,
      sfdc_opportunity_xf.report_opportunity_user_region,
      sfdc_opportunity_xf.report_opportunity_user_area,

      -- NF 2022-02-17 new aggregated keys 
      sfdc_opportunity_xf.report_user_segment_geo_region_area,
      sfdc_opportunity_xf.report_user_segment_geo_region_area_sqs_ot,

      sfdc_opportunity_xf.key_sqs,
      sfdc_opportunity_xf.key_ot,

      sfdc_opportunity_xf.key_segment,
      sfdc_opportunity_xf.key_segment_sqs,                 
      sfdc_opportunity_xf.key_segment_ot,    

      sfdc_opportunity_xf.key_segment_geo,
      sfdc_opportunity_xf.key_segment_geo_sqs,
      sfdc_opportunity_xf.key_segment_geo_ot,      

      sfdc_opportunity_xf.key_segment_geo_region,
      sfdc_opportunity_xf.key_segment_geo_region_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_ot,   

      sfdc_opportunity_xf.key_segment_geo_region_area,
      sfdc_opportunity_xf.key_segment_geo_region_area_sqs,
      sfdc_opportunity_xf.key_segment_geo_region_area_ot,

      sfdc_opportunity_xf.key_segment_geo_area,
      
      -- using current opportunity perspective instead of historical
      -- NF 2021-01-26: this might change to order type live 2.1    
      -- NF 2022-01-28: Update to OT 2.3 will be stamped directly  
      sfdc_opportunity_xf.order_type_stamped,     

      -- top level grouping of the order type field
      sfdc_opportunity_xf.deal_group,

      -- medium level grouping of the order type field
      sfdc_opportunity_xf.deal_category,
      
      -- duplicates flag
      sfdc_opportunity_xf.is_duplicate_flag                               AS current_is_duplicate_flag,

      -- the owner name in the opportunity is not clean.
      opportunity_owner.name AS opportunity_owner,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------

      -- account driven fields
      sfdc_accounts_xf.account_name,
      sfdc_accounts_xf.tsp_region,
      sfdc_accounts_xf.tsp_sub_region,
      sfdc_accounts_xf.ultimate_parent_sales_segment,
      sfdc_accounts_xf.tsp_max_hierarchy_sales_segment,
      sfdc_accounts_xf.ultimate_parent_account_id,
      upa.account_name                        AS ultimate_parent_account_name,
      sfdc_accounts_xf.ultimate_parent_id,
      sfdc_accounts_xf.is_jihu_account,

      sfdc_accounts_xf.account_owner_user_segment,
      sfdc_accounts_xf.account_owner_user_geo, 
      sfdc_accounts_xf.account_owner_user_region,
      sfdc_accounts_xf.account_owner_user_area,
      -- account_owner_subarea_stamped

      sfdc_accounts_xf.account_demographics_sales_segment AS account_demographics_segment,
      sfdc_accounts_xf.account_demographics_geo,
      sfdc_accounts_xf.account_demographics_region,
      sfdc_accounts_xf.account_demographics_area,
      sfdc_accounts_xf.account_demographics_territory,
      -- account_demographics_subarea_stamped        

      sfdc_accounts_xf.account_demographics_sales_segment    AS upa_demographics_segment,
      sfdc_accounts_xf.account_demographics_geo              AS upa_demographics_geo,
      sfdc_accounts_xf.account_demographics_region           AS upa_demographics_region,
      sfdc_accounts_xf.account_demographics_area             AS upa_demographics_area,
      sfdc_accounts_xf.account_demographics_territory        AS upa_demographics_territory
      

    FROM sfdc_opportunity_snapshot_history opp_snapshot
    INNER JOIN sfdc_opportunity_xf    
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_opportunity_xf.account_id = sfdc_accounts_xf.account_id 
    LEFT JOIN sfdc_accounts_xf upa
      ON upa.account_id = sfdc_accounts_xf.ultimate_parent_account_id
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    WHERE opp_snapshot.raw_account_id NOT IN ('0014M00001kGcORQA0')                           -- remove test account
      AND (sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')
            OR sfdc_accounts_xf.account_id IS NULL)                                        -- remove test account
      AND opp_snapshot.is_deleted = 0
      -- NF 20210906 remove JiHu opties from the models
      AND sfdc_accounts_xf.is_jihu_account = 0

)
-- in Q2 FY21 a few deals where created in the wrong stage, and as they were purely aspirational, 
-- they needed to be removed from stage 1, eventually by the end of the quarter they were removed
-- The goal of this list is to use in the Created Pipeline flag, to exclude those deals that at 
-- day 90 had stages of less than 1, that should smooth the chart
, vision_opps  AS (
  
  SELECT opp_snapshot.opportunity_id,
         opp_snapshot.stage_name,
         opp_snapshot.snapshot_fiscal_quarter_date
  FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
  WHERE opp_snapshot.snapshot_fiscal_quarter_name = 'FY21-Q2'
    And opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
    AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
    AND opp_snapshot.stage_name in ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying')
  GROUP BY 1, 2, 3


), add_compound_metrics AS (

    SELECT 
      opp_snapshot.*

    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
      LEFT JOIN vision_opps
        ON vision_opps.opportunity_id = opp_snapshot.opportunity_id
        AND vision_opps.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date

)

SELECT *
FROM add_compound_metrics
