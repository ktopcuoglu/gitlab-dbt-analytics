{{ config(alias='sfdc_opportunity_snapshot_history_xf') }}
-- TODO
-- Add CS churn fields into model from wk_sales_opportunity object

WITH date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }} 

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}} 


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

      sales_team_cro_level,
      sales_team_rd_asm_level,

      -- Opportunity Owner Stamped fields
      opportunity_owner_user_segment,
      opportunity_owner_user_region,
      opportunity_owner_user_area,
      opportunity_owner_user_geo,

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

), sfdc_opportunity_snapshot_history AS (

    SELECT 
      --sfdc_opportunity_snapshot_history.valid_from,
      --sfdc_opportunity_snapshot_history.valid_to,
      --sfdc_opportunity_snapshot_history.is_currently_valid,
      sfdc_opportunity_snapshot_history.opportunity_snapshot_id,

      -- Accounts might get deleted or merged, I am selecting the latest account id from the opty object
      -- to avoid showing non-valid account ids
      sfdc_opportunity_snapshot_history.account_id AS raw_account_id,
      
      sfdc_opportunity_snapshot_history.opportunity_id,
      sfdc_opportunity_snapshot_history.opportunity_name,
      sfdc_opportunity_snapshot_history.owner_id,
      --sfdc_opportunity_snapshot_history.business_type,
      sfdc_opportunity_snapshot_history.close_date,
      sfdc_opportunity_snapshot_history.created_date,
      sfdc_opportunity_snapshot_history.deployment_preference,
      --sfdc_opportunity_snapshot_history.generated_source,
      sfdc_opportunity_snapshot_history.lead_source,
      sfdc_opportunity_snapshot_history.merged_opportunity_id,
      
      -- NF: This field is added directly from the user table
      -- as the opportunity one is not clean
      --sfdc_opportunity_snapshot_history.opportunity_owner,
 
      sfdc_opportunity_snapshot_history.opportunity_owner_department,
      sfdc_opportunity_snapshot_history.opportunity_sales_development_representative,
      sfdc_opportunity_snapshot_history.opportunity_business_development_representative,
      sfdc_opportunity_snapshot_history.opportunity_development_representative,

      sfdc_opportunity_snapshot_history.order_type AS snapshot_order_type_stamped,
      --sfdc_opportunity_snapshot_history.order_type,
      --sfdc_opportunity_snapshot_history.opportunity_owner_team,
      --sfdc_opportunity_snapshot_history.opportunity_owner_manager,
      --sfdc_opportunity_snapshot_history.account_owner_team_stamped,
      --sfdc_opportunity_snapshot_history.parent_segment,
      sfdc_opportunity_snapshot_history.sales_accepted_date,
      sfdc_opportunity_snapshot_history.sales_path,
      sfdc_opportunity_snapshot_history.sales_qualified_date,
      --sfdc_opportunity_snapshot_history.sales_segment,
      sfdc_opportunity_snapshot_history.sales_type,
      sfdc_opportunity_snapshot_history.net_new_source_categories,
      sfdc_opportunity_snapshot_history.source_buckets,
      sfdc_opportunity_snapshot_history.stage_name,

      sfdc_opportunity_snapshot_history.acv,
      --sfdc_opportunity_snapshot_history.closed_deals,
      sfdc_opportunity_snapshot_history.competitors,
      --sfdc_opportunity_snapshot_history.critical_deal_flag,
      --sfdc_opportunity_snapshot_history.deal_size,
      sfdc_opportunity_snapshot_history.forecast_category_name,
      --sfdc_opportunity_snapshot_history.forecasted_iacv,
      sfdc_opportunity_snapshot_history.iacv_created_date,
      sfdc_opportunity_snapshot_history.incremental_acv,
      sfdc_opportunity_snapshot_history.invoice_number,

      -- logic needs to be added here once the oppotunity category fields is merged
      -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
      CASE
        WHEN sfdc_opportunity_snapshot_history.opportunity_category IN ('Decommission')
          THEN 1
        ELSE 0
      END                                                          AS is_refund,

      CASE
        WHEN sfdc_opportunity_snapshot_history.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                          AS is_credit_flag,
      
      CASE
        WHEN sfdc_opportunity_snapshot_history.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                          AS is_contract_reset_flag,
      --sfdc_opportunity_snapshot_history.is_refund,

      --sfdc_opportunity_snapshot_history.is_downgrade,
      --sfdc_opportunity_snapshot_history.is_swing_deal,
      sfdc_opportunity_snapshot_history.net_incremental_acv,
      --sfdc_opportunity_snapshot_history.nrv,
      sfdc_opportunity_snapshot_history.primary_campaign_source_id,
      --sfdc_opportunity_snapshot_history.probability,
      sfdc_opportunity_snapshot_history.professional_services_value,
      --sfdc_opportunity_snapshot_history.pushed_count,
      --sfdc_opportunity_snapshot_history.reason_for_loss,
      --sfdc_opportunity_snapshot_history.reason_for_loss_details,
      sfdc_opportunity_snapshot_history.refund_iacv,
      sfdc_opportunity_snapshot_history.downgrade_iacv,
      sfdc_opportunity_snapshot_history.renewal_acv,
      sfdc_opportunity_snapshot_history.renewal_amount,
      --sfdc_opportunity_snapshot_history.sales_qualified_source,
      --sfdc_opportunity_snapshot_history.segment,
      --sfdc_opportunity_snapshot_history.solutions_to_be_replaced,
      sfdc_opportunity_snapshot_history.total_contract_value,
      --sfdc_opportunity_snapshot_history.upside_iacv,
      --sfdc_opportunity_snapshot_history.upside_swing_deal_iacv,
      sfdc_opportunity_snapshot_history.is_web_portal_purchase,
      sfdc_opportunity_snapshot_history.opportunity_term,
      
      sfdc_opportunity_snapshot_history.net_arr             AS raw_net_arr,
      
      --sfdc_opportunity_snapshot_history.user_segment_stamped,
      --sfdc_opportunity_snapshot_history.user_region_stamped,
      --sfdc_opportunity_snapshot_history.user_area_stamped,
      --sfdc_opportunity_snapshot_history.user_geo_stamped,
      
      sfdc_opportunity_snapshot_history.arr_basis,
      sfdc_opportunity_snapshot_history.arr,
      sfdc_opportunity_snapshot_history.amount,
      sfdc_opportunity_snapshot_history.recurring_amount,
      sfdc_opportunity_snapshot_history.true_up_amount,
      sfdc_opportunity_snapshot_history.proserv_amount,
      sfdc_opportunity_snapshot_history.other_non_recurring_amount,
      sfdc_opportunity_snapshot_history.subscription_start_date,
      sfdc_opportunity_snapshot_history.subscription_end_date,
      /*
      sfdc_opportunity_snapshot_history.cp_champion,
      sfdc_opportunity_snapshot_history.cp_close_plan,
      sfdc_opportunity_snapshot_history.cp_competition,
      sfdc_opportunity_snapshot_history.cp_decision_criteria,
      sfdc_opportunity_snapshot_history.cp_decision_process,
      sfdc_opportunity_snapshot_history.cp_economic_buyer,
      sfdc_opportunity_snapshot_history.cp_identify_pain,
      sfdc_opportunity_snapshot_history.cp_metrics,
      sfdc_opportunity_snapshot_history.cp_risks,
      */
      sfdc_opportunity_snapshot_history.cp_use_cases,
      /*sfdc_opportunity_snapshot_history.cp_value_driver,
      sfdc_opportunity_snapshot_history.cp_why_do_anything_at_all,
      sfdc_opportunity_snapshot_history.cp_why_gitlab,
      sfdc_opportunity_snapshot_history.cp_why_now,
      */
      sfdc_opportunity_snapshot_history._last_dbt_run,
      sfdc_opportunity_snapshot_history.is_deleted,
      sfdc_opportunity_snapshot_history.last_activity_date,
      sfdc_opportunity_snapshot_history.record_type_id,
      --sfdc_opportunity_snapshot_history.opportunity_category,

      -- Channel Org. fields
      -- this fields should be changed to this historical version
      sfdc_opportunity_snapshot_history.deal_path,
      sfdc_opportunity_snapshot_history.dr_partner_deal_type,
      sfdc_opportunity_snapshot_history.dr_partner_engagement,
      sfdc_opportunity_snapshot_history.partner_account,
      sfdc_opportunity_snapshot_history.dr_status,
      sfdc_opportunity_snapshot_history.distributor,
      sfdc_opportunity_snapshot_history.influence_partner,
      sfdc_opportunity_snapshot_history.fulfillment_partner,
      sfdc_opportunity_snapshot_history.platform_partner,
      sfdc_opportunity_snapshot_history.partner_track,
      sfdc_opportunity_snapshot_history.is_public_sector_opp,
      sfdc_opportunity_snapshot_history.is_registration_from_portal,
      sfdc_opportunity_snapshot_history.calculated_discount,
      sfdc_opportunity_snapshot_history.partner_discount,
      sfdc_opportunity_snapshot_history.partner_discount_calc,
      sfdc_opportunity_snapshot_history.comp_channel_neutral,

      sfdc_opportunity_snapshot_history.fpa_master_bookings_flag,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.deal_path = 'Direct'
          THEN 'Direct'
        WHEN sfdc_opportunity_snapshot_history.deal_path = 'Web Direct'
          THEN 'Web Direct' 
        WHEN sfdc_opportunity_snapshot_history.deal_path = 'Channel' 
            AND sfdc_opportunity_snapshot_history.sales_qualified_source = 'Channel Generated' 
          THEN 'Partner Sourced'
        WHEN sfdc_opportunity_snapshot_history.deal_path = 'Channel' 
            AND sfdc_opportunity_snapshot_history.sales_qualified_source != 'Channel Generated' 
          THEN 'Partner Co-Sell'
      END                                                         AS deal_path_engagement,


      -- stage dates
            -- dates in stage fields
      sfdc_opportunity_snapshot_history.stage_0_pending_acceptance_date,
      sfdc_opportunity_snapshot_history.stage_1_discovery_date,
      sfdc_opportunity_snapshot_history.stage_2_scoping_date,
      sfdc_opportunity_snapshot_history.stage_3_technical_evaluation_date,
      sfdc_opportunity_snapshot_history.stage_4_proposal_date,
      sfdc_opportunity_snapshot_history.stage_5_negotiating_date,
      sfdc_opportunity_snapshot_history.stage_6_awaiting_signature_date,
      sfdc_opportunity_snapshot_history.stage_6_closed_won_date,
      sfdc_opportunity_snapshot_history.stage_6_closed_lost_date,

      --date helpers

      sfdc_opportunity_snapshot_history.date_actual               AS snapshot_date,  
      snapshot_date.first_day_of_month                            AS snapshot_date_month,
      snapshot_date.fiscal_year                                   AS snapshot_fiscal_year,
      snapshot_date.fiscal_quarter_name_fy                        AS snapshot_fiscal_quarter_name,
      snapshot_date.first_day_of_fiscal_quarter                   AS snapshot_fiscal_quarter_date,
      snapshot_date.day_of_fiscal_quarter_normalised              AS snapshot_day_of_fiscal_quarter_normalised,
      
      close_date_detail.first_day_of_month                        AS close_date_month,
      close_date_detail.fiscal_year                               AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                    AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter               AS close_fiscal_quarter_date,

      -- This refers to the closing quarter perspective instead of the snapshot quarter
      90 - DATEDIFF(day, snapshot_date.date_actual, close_date_detail.last_day_of_fiscal_quarter)           AS close_day_of_fiscal_quarter_normalised,

      created_date_detail.first_day_of_month                      AS created_date_month,
      created_date_detail.fiscal_year                             AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                  AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter             AS created_fiscal_quarter_date,

      net_arr_created_date.first_day_of_month                     AS iacv_created_date_month,
      net_arr_created_date.fiscal_year                            AS iacv_created_fiscal_year,
      net_arr_created_date.fiscal_quarter_name_fy                 AS iacv_created_fiscal_quarter_name,
      net_arr_created_date.first_day_of_fiscal_quarter            AS iacv_created_fiscal_quarter_date,

      created_date_detail.date_actual                             AS net_arr_created_date,
      created_date_detail.first_day_of_month                      AS net_arr_created_date_month,
      created_date_detail.fiscal_year                             AS net_arr_created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                  AS net_arr_created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter             AS net_arr_created_fiscal_quarter_date,

      net_arr_created_date.date_actual                            AS pipeline_created_date,
      net_arr_created_date.first_day_of_month                     AS pipeline_created_date_month,
      net_arr_created_date.fiscal_year                            AS pipeline_created_fiscal_year,
      net_arr_created_date.fiscal_quarter_name_fy                 AS pipeline_created_fiscal_quarter_name,
      net_arr_created_date.first_day_of_fiscal_quarter            AS pipeline_created_fiscal_quarter_date,

      sales_accepted_date.first_day_of_month                     AS sales_accepted_date_month,
      sales_accepted_date.fiscal_year                            AS sales_accepted_date_fiscal_year,
      sales_accepted_date.fiscal_quarter_name_fy                 AS sales_accepted_date_fiscal_quarter_name,
      sales_accepted_date.first_day_of_fiscal_quarter            AS sales_accepted_date_fiscal_quarter_date,
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                              ,'Developing', '1-Discovery', '2-Developing', '2-Scoping')  
          THEN 'Pipeline'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating'
                              , '6-Awaiting Signature', '7-Closing')                         
          THEN '3+ Pipeline'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       
          THEN 'Lost'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('Closed Won')                                                                                                         
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_3plus,
      
      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                            , 'Developing', '1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
          THEN 'Pipeline'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               
          THEN '4+ Pipeline'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   
          THEN 'Lost'
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('Closed Won')                                                                                                                                     
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_4plus,


      CASE
        WHEN sfdc_opportunity_snapshot_history.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                         AS is_stage_1_plus,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_3_plus,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name 
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_4_plus,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name = 'Closed Won' 
          THEN 1 ELSE 0
      END                                                         AS is_won,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('8-Closed Lost', 'Closed Lost')  
          THEN 1 ELSE 0
      END                                                         AS is_lost,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1  
      END                                                         AS is_open,

      CASE 
        WHEN sfdc_opportunity_snapshot_history.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
          THEN 1
        ELSE 0
      END                                                         AS is_closed,
      

      CASE 
        WHEN LOWER(sfdc_opportunity_snapshot_history.sales_type) like '%renewal%' 
          THEN 1
        ELSE 0
      END                                                         AS is_renewal,


      -- NF: 20210827 Fields for competitor analysis 
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Other') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_other_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'GitLab Core') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_gitlab_core_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'None') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_none_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'GitHub Enterprise') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_github_enterprise_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'BitBucket Server') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bitbucket_server_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Unknown') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_unknown_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'GitHub.com') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_github_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'GitLab.com') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_gitlab_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Jenkins') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_jenkins_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Azure DevOps') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_azure_devops_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'SVN') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_svn_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'BitBucket.Org') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bitbucket_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Atlassian') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_atlassian_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Perforce') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_perforce_flag, 
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Visual Studio Team Services') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_visual_studio_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Azure') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_azure_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Amazon Code Commit') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_amazon_code_commit_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'CircleCI') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_circleci_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'Bamboo') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bamboo_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_snapshot_history.competitors, 'AWS') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_aws_flag,


      -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      CASE
        WHEN is_open = 1
          THEN DATEDIFF(days, created_date_detail.date_actual, snapshot_date.date_actual)
        WHEN is_open = 0 AND snapshot_date.date_actual < close_date_detail.date_actual
          THEN DATEDIFF(days, created_date_detail.date_actual, snapshot_date.date_actual)
        ELSE DATEDIFF(days, created_date_detail.date_actual, close_date_detail.date_actual)
      END                                                       AS calculated_age_in_days

    FROM {{ref('sfdc_opportunity_snapshot_history')}}
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = sfdc_opportunity_snapshot_history.close_date::DATE
    INNER JOIN date_details snapshot_date
      ON sfdc_opportunity_snapshot_history.date_actual::DATE = snapshot_date.date_actual
    LEFT JOIN date_details created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity_snapshot_history.created_date::DATE
    LEFT JOIN date_details net_arr_created_date
      ON net_arr_created_date.date_actual = sfdc_opportunity_snapshot_history.iacv_created_date::DATE
    LEFT JOIN date_details sales_accepted_date
      ON sales_accepted_date.date_actual = sfdc_opportunity_snapshot_history.sales_accepted_date::DATE


), net_iacv_to_net_arr_ratio AS (

    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.999691784               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.998590143               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.992289340               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.927846192               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.852915435               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          1.009262672               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.793618079               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.988527875               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          1.010081083               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.000000000               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.002741689               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          0.965670500               AS "RATIO_NET_IACV_TO_NET_ARR" 

), sfdc_opportunity_snapshot_history_xf AS (

  SELECT DISTINCT

      opp_snapshot.*,

      CASE 
        WHEN opp_snapshot.is_won = 1  
          THEN '1.Won'
        WHEN opp_snapshot.is_lost = 1 
          THEN '2.Lost'
        WHEN opp_snapshot.is_open = 1 
          THEN '0. Open' 
        ELSE 'N/A'
      END                                                         AS stage_category,
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      
      -- Historical Net ARR Logic Summary   
      -- closed deals use net_incremental_acv
      -- open deals use incremental acv
      -- closed won deals with net_arr > 0 use that opportunity calculated ratio
      -- deals with no opty with net_arr use a default ratio for segment / order type
      -- deals before 2021-02-01 use always net_arr calculated from ratio
      -- deals after 2021-02-01 use net_arr if > 0, if open and not net_arr uses ratio version

      -- If the opportunity exists, use the ratio from the opportunity sheetload
      -- I am faking that using the opportunity table directly
      CASE 
        WHEN sfdc_opportunity_xf.is_won = 1 -- only consider won deals
          AND sfdc_opportunity_xf.opportunity_category <> 'Contract Reset' -- contract resets have a special way of calculating net iacv
          AND COALESCE(sfdc_opportunity_xf.raw_net_arr,0) <> 0
          AND COALESCE(sfdc_opportunity_xf.net_incremental_acv,0) <> 0
            THEN COALESCE(sfdc_opportunity_xf.raw_net_arr / sfdc_opportunity_xf.net_incremental_acv,0)
        ELSE NULL 
      END                                                                     AS opportunity_based_iacv_to_net_arr_ratio,
     
      -- If there is no opportnity, use a default table ratio
      -- I am faking that using the upper CTE, that should be replaced by the official table
      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)         AS segment_order_type_iacv_to_net_arr_ratio,

      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- if there is an opportunity based ratio, use that, if not, use default from segment / order type

      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE 
        WHEN opp_snapshot.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(opp_snapshot.net_incremental_acv,0) = 0
            THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(opp_snapshot.net_incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,
      
      -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
      -- Those were later fixed in the opportunity object but stayed in the snapshot table.
      -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
      CASE
        WHEN  opp_snapshot.snapshot_date < '2021-02-01'::DATE -- All deals before cutoff and that were not updated to Net ARR
          THEN calculated_from_ratio_net_arr
        WHEN  opp_snapshot.snapshot_date >= '2021-02-01'::DATE  -- After cutoff date, for all deals earlier than FY19 that are closed and have no net arr
              AND opp_snapshot.close_date < '2018-02-01'::DATE 
              AND opp_snapshot.is_open = 0
              AND COALESCE(opp_snapshot.raw_net_arr,0) = 0 
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(opp_snapshot.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,
      
      ------------------------------
      -- fields for counting new logos, these fields count refund as negative
      CASE 
        WHEN opp_snapshot.is_refund = 1
          THEN -1
        WHEN opp_snapshot.is_credit_flag = 1
          THEN 0
        ELSE 1
      END                                                                     AS calculated_deal_count,

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

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- DEPRECATED IACV METRICS
      -- Use Net ARR instead
      CASE 
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name= opp_snapshot.close_fiscal_quarter_name
          AND opp_snapshot.is_won = 1 
            THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                         AS created_and_won_same_quarter_iacv,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          THEN opp_snapshot.incremental_acv 
        ELSE 0 
      END                                                         AS created_in_snapshot_quarter_iacv,

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
      sfdc_opportunity_xf.opportunity_owner_user_segment,
      sfdc_opportunity_xf.opportunity_owner_user_region,
      sfdc_opportunity_xf.opportunity_owner_user_area,
      sfdc_opportunity_xf.opportunity_owner_user_geo,

      --- target fields for reporting, changing their name might help to isolate their logic from the actual field
      -- in FY21, there were multiple ways of getting this done, and it meant changing downwards reports
      sfdc_opportunity_xf.sales_team_cro_level,
      sfdc_opportunity_xf.sales_team_rd_asm_level,
      
      -- using current opportunity perspective instead of historical
      -- NF 2020-01-26: this might change to order type live 2.1     
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
      sfdc_accounts_xf.is_jihu_account        
      

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
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = sfdc_opportunity_xf.opportunity_owner_user_segment
      AND net_iacv_to_net_arr_ratio.order_type_stamped = sfdc_opportunity_xf.order_type_stamped
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
      opp_snapshot.*,

      ------------------------------
      -- compound metrics for reporting
      ------------------------------

      -- current deal size field, it was creasted by the data team and the original doesn't work
      CASE 
        WHEN opp_snapshot.net_arr > 0 AND net_arr < 5000 
          THEN '1 - Small (<5k)'
        WHEN opp_snapshot.net_arr >=5000 AND net_arr < 25000 
          THEN '2 - Medium (5k - 25k)'
        WHEN opp_snapshot.net_arr >=25000 AND net_arr < 100000 
          THEN '3 - Big (25k - 100k)'
        WHEN opp_snapshot.net_arr >= 100000 
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other' 
      END                                                          AS deal_size,

      -- extended version of the deal size
      CASE 
        WHEN net_arr > 0 AND net_arr < 1000 
          THEN '1. (0k -1k)'
        WHEN net_arr >=1000 AND net_arr < 10000 
          THEN '2. (1k - 10k)'
        WHEN net_arr >=10000 AND net_arr < 50000 
          THEN '3. (10k - 50k)'
        WHEN net_arr >=50000 AND net_arr < 100000 
          THEN '4. (50k - 100k)'
        WHEN net_arr >= 100000 AND net_arr < 250000 
          THEN '5. (100k - 250k)'
        WHEN net_arr >= 250000 AND net_arr < 500000 
          THEN '6. (250k - 500k)'
        WHEN net_arr >= 500000 AND net_arr < 1000000 
          THEN '7. (500k-1000k)'
        WHEN net_arr >= 1000000 
          THEN '8. (>1000k)'
        ELSE 'Other' 
      END                                                           AS calculated_deal_size,

            -- Open pipeline eligibility definition
      CASE 
        WHEN lower(opp_snapshot.deal_group) LIKE ANY ('%growth%', '%new%')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_stage_1_plus = 1
          AND opp_snapshot.forecast_category_name != 'Omitted'
          AND opp_snapshot.is_open = 1
         THEN 1
         ELSE 0
      END                                                   AS is_eligible_open_pipeline_flag,


      -- Created pipeline eligibility definition
      CASE 
        WHEN opp_snapshot.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND opp_snapshot.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')  
          AND ((opp_snapshot.is_stage_1_plus = 1
                AND opp_snapshot.forecast_category_name != 'Omitted')
            OR opp_snapshot.is_lost = 1)
          AND (opp_snapshot.net_arr > 0 
            OR opp_snapshot.opportunity_category = 'Credit')
          -- exclude vision opps from FY21-Q2
          AND (opp_snapshot.pipeline_created_fiscal_quarter_name != 'FY21-Q2'
                OR vision_opps.opportunity_id IS NULL)
          -- 20210802 remove webpurchase deals
          AND opp_snapshot.is_web_portal_purchase = 0
             THEN 1
         ELSE 0
      END                                                      AS is_eligible_created_pipeline_flag,

      CASE
        WHEN opp_snapshot.sales_accepted_date IS NOT NULL
          AND opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
            THEN 1
        ELSE 0
      END                                                     AS is_eligible_sao_flag,

      CASE 
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND opp_snapshot.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Web Purchase have no Net ARR before reconciliation, exclude those 
          -- from ASP analysis
          AND ((opp_snapshot.is_web_portal_purchase = 1 
                AND net_arr > 0)
                OR opp_snapshot.is_web_portal_purchase = 0)
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                    AS is_eligible_asp_analysis_flag,
      
      CASE 
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          -- Renewals are not having the same motion as rest of deals
          AND opp_snapshot.is_renewal = 0
          -- For stage age we exclude only ps/other
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Only include deal types with meaningful journeys through the stages
          AND opp_snapshot.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          -- Web Purchase have a different dynamic and should not be included
          AND opp_snapshot.is_web_portal_purchase = 0
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_age_analysis_flag,

      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          AND (opp_snapshot.is_won = 1 
              OR (opp_snapshot.is_renewal = 1 AND opp_snapshot.is_lost = 1))
          AND opp_snapshot.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                   AS is_eligible_net_arr_flag,

      CASE
        WHEN opp_snapshot.is_edu_oss = 0
          AND opp_snapshot.is_deleted = 0
          AND opp_snapshot.order_type_stamped IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Not JiHu
            THEN 1
          ELSE 0
      END                                                  AS is_eligible_churn_contraction_flag,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.net_arr
        ELSE 0 
      END                                                 AS created_in_snapshot_quarter_net_arr,

   -- created and closed within the quarter net arr
      CASE 
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.close_fiscal_quarter_name
           AND is_won = 1
           AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                 AS created_and_won_same_quarter_net_arr,


      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          AND is_eligible_created_pipeline_flag = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0 
      END                                                 AS created_in_snapshot_quarter_deal_count,

      ---------------------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------------------
      -- Fields created to simplify report building down the road. Specially the pipeline velocity.

      -- deal count
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_1_plus = 1
            THEN opp_snapshot.calculated_deal_count  
        ELSE 0                                                                                              
      END                                               AS open_1plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_3_plus = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_4_plus = 1
            THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,

      -- booked deal count
      CASE 
        WHEN opp_snapshot.is_won = 1
          THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,
    
      -- churned contraction deal count as OT
      CASE
        WHEN ((opp_snapshot.is_renewal = 1
            AND opp_snapshot.is_lost = 1)
            OR opp_snapshot.is_won = 1 )
            AND opp_snapshot.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN opp_snapshot.calculated_deal_count
        ELSE 0
      END                                               AS churned_contraction_deal_count,

      -----------------
      -- Net ARR

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          THEN opp_snapshot.net_arr
        ELSE 0                                                                                              
      END                                                AS open_1plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND opp_snapshot.is_stage_3_plus = 1   
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,
  
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1  
          AND opp_snapshot.is_stage_4_plus = 1
            THEN opp_snapshot.net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN (opp_snapshot.is_won = 1 
            OR (opp_snapshot.is_renewal = 1 
                  AND opp_snapshot.is_lost = 1))
          THEN opp_snapshot.net_arr
        ELSE 0 
      END                                                 AS booked_net_arr,

      -- churned contraction deal count as OT
      CASE
        WHEN ((opp_snapshot.is_renewal = 1
            AND opp_snapshot.is_lost = 1)
            OR opp_snapshot.is_won = 1 )
            AND opp_snapshot.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,

      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN opp_snapshot.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND opp_snapshot.close_date < '2020-08-01' 
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN opp_snapshot.opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          AND opp_snapshot.snapshot_date < '2021-05-01' 
          THEN 1
        -- exclude vision opps from FY21-Q2
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = 'FY21-Q2'
                AND vision_opps.opportunity_id IS NOT NULL
          THEN 1
        ELSE 0
      END                                                         AS is_excluded_flag


    FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
      LEFT JOIN vision_opps
        ON vision_opps.opportunity_id = opp_snapshot.opportunity_id
        AND vision_opps.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date

)

SELECT *
FROM add_compound_metrics
