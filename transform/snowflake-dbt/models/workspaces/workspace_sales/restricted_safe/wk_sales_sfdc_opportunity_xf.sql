{{ config(alias='sfdc_opportunity_xf') }}

-- +wk_sales_report_pipeline_metrics_per_day_with_targets +wk_sales_report_pipeline_velocity_quarter_with_targets +wk_sales_report_opportunity_pipeline_type

WITH sfdc_opportunity AS (

    SELECT opportunity_id,
          opportunity_category
    FROM {{ref('sfdc_opportunity')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('wk_sales_sfdc_users_xf')}}

), sfdc_accounts_xf AS (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }} 

), sfdc_opportunity_xf AS (

    SELECT 
      sfdc_opportunity_xf.account_id,
      sfdc_opportunity_xf.opportunity_id,
      sfdc_opportunity_xf.opportunity_name,
      sfdc_opportunity_xf.owner_id,
      sfdc_opportunity_xf.close_date,
      sfdc_opportunity_xf.created_date,
      sfdc_opportunity_xf.days_in_stage,
      sfdc_opportunity_xf.deployment_preference,
      sfdc_opportunity_xf.generated_source,
      sfdc_opportunity_xf.lead_source,
      sfdc_opportunity_xf.lead_source_id,
      sfdc_opportunity_xf.lead_source_name,
      sfdc_opportunity_xf.lead_source_type,
      sfdc_opportunity_xf.merged_opportunity_id,
      sfdc_opportunity_xf.net_new_source_categories,
      sfdc_opportunity_xf.opportunity_business_development_representative,
      opportunity_owner.name                            AS opportunity_owner,
      sfdc_opportunity_xf.opportunity_owner_department,
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.opportunity_owner_role,
      sfdc_opportunity_xf.opportunity_owner_team,
      sfdc_opportunity_xf.opportunity_owner_title,
      sfdc_opportunity_xf.opportunity_sales_development_representative,
      sfdc_opportunity_xf.opportunity_development_representative,
      sfdc_opportunity_xf.opportunity_term,
      sfdc_opportunity_xf.primary_campaign_source_id,
      sfdc_opportunity_xf.sales_accepted_date,
      sfdc_opportunity_xf.sales_path,
      sfdc_opportunity_xf.sales_qualified_date,
      sfdc_opportunity_xf.sales_type,
      sfdc_opportunity_xf.sdr_pipeline_contribution,
      sfdc_opportunity_xf.source_buckets,
      sfdc_opportunity_xf.stage_name,
      sfdc_opportunity_xf.stage_is_active,
      sfdc_opportunity_xf.stage_is_closed,
      sfdc_opportunity_xf.technical_evaluation_date,

      sfdc_opportunity_xf.acv,
      sfdc_opportunity_xf.amount,
      sfdc_opportunity_xf.closed_deals,
      sfdc_opportunity_xf.competitors,
      sfdc_opportunity_xf.critical_deal_flag,
      sfdc_opportunity_xf.fpa_master_bookings_flag,

      -- Deal Size field is wrong in the source object
      -- it is using
      -- sfdc_opportunity_xf.deal_size,    
      
      sfdc_opportunity_xf.forecast_category_name,
      sfdc_opportunity_xf.forecasted_iacv,
      sfdc_opportunity_xf.incremental_acv,
      sfdc_opportunity_xf.invoice_number,

      -- logic needs to be added here once the oppotunity category fields is merged
      -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
      --sfdc_opportunity_xf.is_refund,

      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Decommission')
          THEN 1
        ELSE 0
      END                                                          AS is_refund,

      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                          AS is_credit_flag,
      
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                          AS is_contract_reset_flag,
  
      sfdc_opportunity_xf.is_downgrade,
      sfdc_opportunity_xf.is_edu_oss,
      CAST(sfdc_opportunity_xf.is_won AS INTEGER)                   AS is_won,
      sfdc_opportunity_xf.net_incremental_acv,
      sfdc_opportunity_xf.professional_services_value,
      sfdc_opportunity_xf.reason_for_loss,
      sfdc_opportunity_xf.reason_for_loss_details,
      sfdc_opportunity_xf.downgrade_reason,
      sfdc_opportunity_xf.renewal_acv,
      sfdc_opportunity_xf.renewal_amount,
      CASE
        WHEN sfdc_opportunity_xf.sales_qualified_source = 'BDR Generated'
            THEN 'SDR Generated'
        ELSE COALESCE(sfdc_opportunity_xf.sales_qualified_source,'NA')
      END                                                           AS sales_qualified_source,

      sfdc_opportunity_xf.solutions_to_be_replaced,
      sfdc_opportunity_xf.total_contract_value,
      sfdc_opportunity_xf.upside_iacv,
      sfdc_opportunity_xf.is_web_portal_purchase,
      sfdc_opportunity_xf.subscription_start_date,
      sfdc_opportunity_xf.subscription_end_date,
  
      -----------------------------------------------------------
      -----------------------------------------------------------
      -- New fields for FY22 - including user segment / region fields

      sfdc_opportunity_xf.order_type_live,
      sfdc_opportunity_xf.order_type_stamped,

      COALESCE(sfdc_opportunity_xf.net_arr,0)                 AS raw_net_arr,
      sfdc_opportunity_xf.recurring_amount,
      sfdc_opportunity_xf.true_up_amount,
      sfdc_opportunity_xf.proserv_amount,
      sfdc_opportunity_xf.other_non_recurring_amount,
      sfdc_opportunity_xf.arr_basis,
      sfdc_opportunity_xf.arr,

      -----------------------------------------------------------
      -----------------------------------------------------------

      sfdc_opportunity_xf.opportunity_health,
      sfdc_opportunity_xf.is_risky,
      sfdc_opportunity_xf.risk_type,
      sfdc_opportunity_xf.risk_reasons,
      sfdc_opportunity_xf.tam_notes,
      sfdc_opportunity_xf.days_in_1_discovery,
      sfdc_opportunity_xf.days_in_2_scoping,
      sfdc_opportunity_xf.days_in_3_technical_evaluation,
      sfdc_opportunity_xf.days_in_4_proposal,
      sfdc_opportunity_xf.days_in_5_negotiating,
      sfdc_opportunity_xf.stage_0_pending_acceptance_date,
      sfdc_opportunity_xf.stage_1_discovery_date,
      sfdc_opportunity_xf.stage_2_scoping_date,
      sfdc_opportunity_xf.stage_3_technical_evaluation_date,
      sfdc_opportunity_xf.stage_4_proposal_date,
      sfdc_opportunity_xf.stage_5_negotiating_date,
      sfdc_opportunity_xf.stage_6_awaiting_signature_date,
      sfdc_opportunity_xf.stage_6_closed_won_date,
      sfdc_opportunity_xf.stage_6_closed_lost_date,
      sfdc_opportunity_xf.cp_champion,
      sfdc_opportunity_xf.cp_close_plan,
      sfdc_opportunity_xf.cp_competition,
      sfdc_opportunity_xf.cp_decision_criteria,
      sfdc_opportunity_xf.cp_decision_process,
      sfdc_opportunity_xf.cp_economic_buyer,
      sfdc_opportunity_xf.cp_identify_pain,
      sfdc_opportunity_xf.cp_metrics,
      sfdc_opportunity_xf.cp_risks,
      sfdc_opportunity_xf.cp_use_cases,
      sfdc_opportunity_xf.cp_value_driver,
      sfdc_opportunity_xf.cp_why_do_anything_at_all,
      sfdc_opportunity_xf.cp_why_gitlab,
      sfdc_opportunity_xf.cp_why_now,
      
      -----------------------------------------------------------
      -----------------------------------------------------------

      -- used for segment reporting in FY21 and before
      sfdc_opportunity_xf.account_owner_team_stamped,

      -- NF: why do we need these fields now?
      sfdc_opportunity_xf.division_sales_segment_stamped,
      sfdc_opportunity_xf.tsp_max_hierarchy_sales_segment,
      sfdc_opportunity_xf.division_sales_segment,
      sfdc_opportunity_xf.ultimate_parent_sales_segment,
      sfdc_opportunity_xf.segment,
      sfdc_opportunity_xf.sales_segment,
      sfdc_opportunity_xf.parent_segment,

      -----------------------------------------------------------
      -----------------------------------------------------------
      -- Channel Org. fields
      sfdc_opportunity_xf.deal_path,
      sfdc_opportunity_xf.dr_partner_deal_type,
      sfdc_opportunity_xf.dr_partner_engagement,
      sfdc_opportunity_xf.partner_account       AS partner_account,
      partner_account.account_name              AS partner_account_name,
      sfdc_opportunity_xf.dr_status,
      sfdc_opportunity_xf.distributor,

      sfdc_opportunity_xf.influence_partner,

      ----------------------------------------------------------
      -- NF 20211108 this field should be removed when possible, need to validate with Channel Ops
      sfdc_opportunity_xf.fulfillment_partner,
      ----------------------------------------------------------
      sfdc_opportunity_xf.fulfillment_partner   AS resale_partner_id,
      resale_account.account_name               AS resale_partner_name,
      sfdc_opportunity_xf.platform_partner,

      CASE
        WHEN sfdc_opportunity_xf.deal_path = 'Channel' 
          THEN REPLACE(COALESCE(sfdc_opportunity_xf.partner_track,partner_account.partner_track, resale_account.partner_track,'Open'),'select','Select')
        ELSE 'Direct' 
      END                                                                                           AS calculated_partner_track,

      
      sfdc_opportunity_xf.partner_track                                                             AS partner_track,
      partner_account.gitlab_partner_program                                                        AS partner_gitlab_program,

      sfdc_opportunity_xf.is_public_sector_opp,
      sfdc_opportunity_xf.is_registration_from_portal,
      sfdc_opportunity_xf.calculated_discount,
      sfdc_opportunity_xf.partner_discount,
      sfdc_opportunity_xf.partner_discount_calc,
      sfdc_opportunity_xf.comp_channel_neutral,
    
      CASE 
        WHEN sfdc_opportunity_xf.deal_path = 'Direct'
          THEN 'Direct'
        WHEN sfdc_opportunity_xf.deal_path = 'Web Direct'
          THEN 'Web Direct' 
        WHEN sfdc_opportunity_xf.deal_path = 'Channel' 
            AND sfdc_opportunity_xf.sales_qualified_source = 'Channel Generated' 
          THEN 'Partner Sourced'
        WHEN sfdc_opportunity_xf.deal_path = 'Channel' 
            AND sfdc_opportunity_xf.sales_qualified_source != 'Channel Generated' 
          THEN 'Partner Co-Sell'
      END                                                         AS deal_path_engagement,


      sfdc_opportunity_xf.stage_name_3plus,
      sfdc_opportunity_xf.stage_name_4plus,
      sfdc_opportunity_xf.is_stage_3_plus,
      sfdc_opportunity_xf.is_lost,
      
      -- NF: Added the 'Duplicate' stage to the is_open definition
      --sfdc_opportunity_xf.is_open,
      CASE 
        WHEN sfdc_opportunity_xf.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1  
      END                                                         AS is_open,
      
      CASE 
        WHEN sfdc_opportunity_xf.stage_name IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                         AS is_duplicate_flag,

      sfdc_opportunity_xf.is_closed,
      sfdc_opportunity_xf.stage_category,
      sfdc_opportunity_xf.is_renewal,
      sfdc_opportunity_xf.close_fiscal_quarter_name,
      sfdc_opportunity_xf.close_fiscal_quarter_date,
      sfdc_opportunity_xf.close_fiscal_year,
      sfdc_opportunity_xf.close_date_month,
      sfdc_opportunity_xf.created_fiscal_quarter_name,
      sfdc_opportunity_xf.created_fiscal_quarter_date,
      sfdc_opportunity_xf.created_fiscal_year,
      sfdc_opportunity_xf.created_date_month,
      sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_name,
      sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date,
      sfdc_opportunity_xf.subscription_start_date_fiscal_year,
      sfdc_opportunity_xf.subscription_start_date_month,
      sfdc_opportunity_xf.sales_accepted_fiscal_quarter_name,
      sfdc_opportunity_xf.sales_accepted_fiscal_quarter_date,
      sfdc_opportunity_xf.sales_accepted_fiscal_year,
      sfdc_opportunity_xf.sales_accepted_date_month,
      sfdc_opportunity_xf.sales_qualified_fiscal_quarter_name,
      sfdc_opportunity_xf.sales_qualified_fiscal_quarter_date,
      sfdc_opportunity_xf.sales_qualified_fiscal_year,
      sfdc_opportunity_xf.sales_qualified_date_month,

      -- Net ARR Created Date uses the same old IACV Created date field in SFDC
      -- As long as the field in the legacy model is not renamed, this will work

      sfdc_opportunity_xf.iacv_created_date                   AS net_arr_created_date,
      sfdc_opportunity_xf.iacv_created_fiscal_quarter_name    AS net_arr_created_fiscal_quarter_name,
      sfdc_opportunity_xf.iacv_created_fiscal_quarter_date    AS net_arr_created_fiscal_quarter_date,
      sfdc_opportunity_xf.iacv_created_fiscal_year            AS net_arr_created_fiscal_year,
      sfdc_opportunity_xf.iacv_created_date_month             AS net_arr_created_date_month,

      stage_1_date.date_actual                                AS stage_1_date,
      stage_1_date.first_day_of_month                         AS stage_1_date_month,
      stage_1_date.fiscal_year                                AS stage_1_fiscal_year,
      stage_1_date.fiscal_quarter_name_fy                     AS stage_1_fiscal_quarter_name,
      stage_1_date.first_day_of_fiscal_quarter                AS stage_1_fiscal_quarter_date,

      stage_3_date.date_actual                                AS stage_3_date,
      stage_3_date.first_day_of_month                         AS stage_3_date_month,
      stage_3_date.fiscal_year                                AS stage_3_fiscal_year,
      stage_3_date.fiscal_quarter_name_fy                     AS stage_3_fiscal_quarter_name,
      stage_3_date.first_day_of_fiscal_quarter                AS stage_3_fiscal_quarter_date,

      ------------------------------------

      sfdc_opportunity_xf._last_dbt_run,
      sfdc_opportunity_xf.business_process_id,
      sfdc_opportunity_xf.days_since_last_activity,
      sfdc_opportunity_xf.is_deleted,
      sfdc_opportunity_xf.last_activity_date,
      sfdc_opportunity_xf.record_type_description,
      sfdc_opportunity_xf.record_type_id,
      sfdc_opportunity_xf.record_type_label,
      sfdc_opportunity_xf.record_type_modifying_object_type,
      sfdc_opportunity_xf.record_type_name,
      sfdc_opportunity_xf.region_quota_id,
      sfdc_opportunity_xf.sales_quota_id,

      -----------------------------------------------------------------------------------------------------      
      -----------------------------------------------------------------------------------------------------
      -- Opportunity User fields
      -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields

      sfdc_opportunity_xf.user_area_stamped                                 AS opportunity_owner_user_area,
      sfdc_opportunity_xf.user_geo_stamped                                  AS opportunity_owner_user_geo,

      -- Team Segment / ASM - RD 
      --  stamped field is not maintained for open deals
      CASE WHEN sfdc_opportunity_xf.user_segment_stamped IS NULL 
          THEN opportunity_owner.user_segment 
          ELSE sfdc_opportunity_xf.user_segment_stamped
      END                                                                    AS opportunity_owner_user_segment,

      --  stamped field is not maintained for open deals
      -- NF: 20210707 JB Asked to roll LATAM deals into EAST region
      CASE WHEN sfdc_opportunity_xf.user_region_stamped IS NULL
            AND  opportunity_owner.user_region != 'LATAM'
              THEN opportunity_owner.user_region
          WHEN sfdc_opportunity_xf.user_region_stamped != 'LATAM'
              THEN sfdc_opportunity_xf.user_region_stamped
          WHEN (sfdc_opportunity_xf.user_region_stamped = 'LATAM'
              OR  opportunity_owner.user_region = 'LATAM')
                THEN 'East'
          ELSE 'Other'
      END                                                                    AS opportunity_owner_user_region,


      -- NF: 20210827 Fields for competitor analysis 
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Other') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_other_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'GitLab Core') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_gitlab_core_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'None') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_none_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'GitHub Enterprise') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_github_enterprise_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'BitBucket Server') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bitbucket_server_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Unknown') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_unknown_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'GitHub.com') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_github_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'GitLab.com') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_gitlab_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Jenkins') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_jenkins_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Azure DevOps') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_azure_devops_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'SVN') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_svn_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'BitBucket.Org') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bitbucket_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Atlassian') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_atlassian_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Perforce') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_perforce_flag, 
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Visual Studio Team Services') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_visual_studio_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Azure') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_azure_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Amazon Code Commit') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_amazon_code_commit_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'CircleCI') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_circleci_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'Bamboo') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_bamboo_flag,
      CASE
        WHEN CONTAINS (sfdc_opportunity_xf.competitors, 'AWS') 
          THEN 1 
        ELSE 0
      END                                 AS competitors_aws_flag,


      -----------------------------------------------------------------------------------------------------      
      -----------------------------------------------------------------------------------------------------
      
      --sfdc_opportunity_xf.partner_initiated_opportunity,
      --sfdc_opportunity_xf.true_up_value,
      --sfdc_opportunity_xf.is_swing_deal,
      --sfdc_opportunity_xf.probability,
      --sfdc_opportunity_xf.pushed_count,
      --sfdc_opportunity_xf.refund_iacv,
      --sfdc_opportunity_xf.downgrade_iacv,
      --sfdc_opportunity_xf.upside_swing_deal_iacv,
      --sfdc_opportunity_xf.weighted_iacv,


      -- fields form opportunity source
      sfdc_opportunity.opportunity_category
    
    FROM {{ref('sfdc_opportunity_xf')}} sfdc_opportunity_xf
    -- not all fields are in opportunity xf
    INNER JOIN sfdc_opportunity
      ON sfdc_opportunity.opportunity_id = sfdc_opportunity_xf.opportunity_id
    INNER JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = sfdc_opportunity_xf.owner_id
    -- pipeline creation date
    LEFT JOIN date_details stage_1_date 
      ON stage_1_date.date_actual = sfdc_opportunity_xf.stage_1_discovery_date::date
      -- pipeline creation date
    LEFT JOIN date_details stage_3_date 
      ON stage_3_date.date_actual = sfdc_opportunity_xf.stage_3_technical_evaluation_date::date
    -- partner account details
    LEFT JOIN sfdc_accounts_xf partner_account
      ON partner_account.account_id = sfdc_opportunity_xf.partner_account
    -- NF 20211105 resale partner
    LEFT JOIN sfdc_accounts_xf resale_account
      ON resale_account.account_id = sfdc_opportunity_xf.fulfillment_partner 
   -- NF 20210906 remove JiHu opties from the models
    WHERE sfdc_opportunity_xf.is_jihu_account = 0

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


), churn_metrics AS (

SELECT
    o.opportunity_id,
    NVL(o.reason_for_loss, o.downgrade_reason) AS reason_for_loss_staged,
    CASE 
      WHEN reason_for_loss_staged IN ('Do Nothing','Other','Competitive Loss','Operational Silos') 
        OR reason_for_loss_staged IS NULL 
          THEN 'Unknown'
      WHEN reason_for_loss_staged IN ('Missing Feature','Product value/gaps','Product Value / Gaps',
                                          'Stayed with Community Edition','Budget/Value Unperceived') 
          THEN 'Product Value / Gaps'
      WHEN reason_for_loss_staged IN ('Lack of Engagement / Sponsor','Went Silent','Evangelist Left') 
          THEN 'Lack of Engagement / Sponsor'
      WHEN reason_for_loss_staged IN ('Loss of Budget','No budget') 
          THEN 'Loss of Budget'
      WHEN reason_for_loss_staged = 'Merged into another opportunity' 
          THEN 'Merged Opp'
      WHEN reason_for_loss_staged = 'Stale Opportunity' 
          THEN 'No Progression - Auto-close'
      WHEN reason_for_loss_staged IN ('Product Quality / Availability','Product quality/availability') 
          THEN 'Product Quality / Availability'
      ELSE reason_for_loss_staged
     END                                    AS reason_for_loss_calc,
    o.reason_for_loss_details,
    
    CASE 
      WHEN o.order_type_stamped IN ('4. Contraction','5. Churn - Partial')
        THEN 'Contraction'
      ELSE 'Churn'
    END                                    AS churn_contraction_type_calc

FROM sfdc_opportunity_xf o
WHERE o.order_type_stamped IN ('4. Contraction','5. Churn - Partial','6. Churn - Final')
    AND (o.is_won = 1
        OR (is_renewal = 1 AND is_lost = 1))

), oppty_final AS (

    SELECT 

      sfdc_opportunity_xf.*,

      -- date helpers
      
      -- pipeline created, tracks the date pipeline value was created for the first time
      -- used for performance reporting on pipeline generation
      -- these fields might change, isolating the field from the purpose
      -- alternatives are a future net_arr_created_date
      sfdc_opportunity_xf.net_arr_created_date                        AS pipeline_created_date,
      sfdc_opportunity_xf.net_arr_created_date_month                  AS pipeline_created_date_month,
      sfdc_opportunity_xf.net_arr_created_fiscal_year                 AS pipeline_created_fiscal_year,
      sfdc_opportunity_xf.net_arr_created_fiscal_quarter_name         AS pipeline_created_fiscal_quarter_name,
      sfdc_opportunity_xf.net_arr_created_fiscal_quarter_date         AS pipeline_created_fiscal_quarter_date,

      CASE
        WHEN sfdc_opportunity_xf.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                   AS is_stage_1_plus,


      CASE
        WHEN sfdc_opportunity_xf.stage_name
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                   AS is_stage_4_plus,

      -- account driven fields 
      sfdc_accounts_xf.account_name,
      sfdc_accounts_xf.ultimate_parent_account_id,
      sfdc_accounts_xf.is_jihu_account,

      -- medium level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('4. Contraction')
          THEN '3. Contraction'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('5. Churn - Partial','6. Churn - Final')
          THEN '4. Churn'
        ELSE '5. Other' 
      END                                                                   AS deal_category,


      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction') 
          THEN '2. Growth' 
        ELSE '3. Other'
      END                                                                   AS deal_group,

      ----------------------------------------------------------------
      ----------------------------------------------------------------
      -- temporary, to deal with global Bookings FY21 reports that use account_owner_team_stamp field
      CASE 
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('Commercial - SMB','SMB','SMB - US','SMB - International')
          THEN 'SMB'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('APAC','EMEA','Channel','US West','US East','Public Sector')
          THEN 'Large'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('MM - APAC','MM - East','MM - EMEA','Commercial - MM','MM - West','MM-EMEA')
          THEN 'Mid-Market'
        ELSE 'SMB'
      END                                                                   AS account_owner_team_stamped_cro_level,   

      ----------------------------------------------------------------
      ----------------------------------------------------------------

      -- fields for counting new logos, these fields count refund as negative
      CASE 
        WHEN sfdc_opportunity_xf.is_refund = 1
          THEN -1
        WHEN sfdc_opportunity_xf.is_credit_flag = 1
          THEN 0
        ELSE 1
      END                                                                    AS calculated_deal_count,

        -- PIO Flag for PIO reporting dashboard
      CASE 
        WHEN sfdc_opportunity_xf.dr_partner_engagement = 'PIO' 
          THEN 1 
        ELSE 0 
      END                                                                    AS partner_engaged_opportunity_flag,

      
       -- check if renewal was closed on time or not
      CASE 
        WHEN sfdc_opportunity_xf.is_renewal = 1 
          AND sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date >= sfdc_opportunity_xf.close_fiscal_quarter_date 
            THEN 'On-Time'
        WHEN sfdc_opportunity_xf.is_renewal = 1 
          AND sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date < sfdc_opportunity_xf.close_fiscal_quarter_date 
            THEN 'Late' 
      END                                                                       AS renewal_timing_status,

      --********************************************************
      -- calculated fields for pipeline velocity report
      
      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN sfdc_accounts_xf.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND sfdc_opportunity_xf.close_date < '2020-08-01' 
            THEN 1
        -- NF 2021 - Pubsec extreme deals
        WHEN sfdc_opportunity_xf.opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
          THEN 1
        ELSE 0
      END                                                                       AS is_excluded_flag,

      -- Customer Success related fields
      -- DRI Michael Armtz
      churn_metrics.reason_for_loss_staged,
      churn_metrics.reason_for_loss_calc,
      churn_metrics.churn_contraction_type_calc

    FROM sfdc_opportunity_xf
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_accounts_xf.account_id = sfdc_opportunity_xf.account_id
    LEFT JOIN churn_metrics 
      ON churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id
    
    WHERE sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')   -- remove test account
      AND sfdc_opportunity_xf.account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
      AND sfdc_opportunity_xf.is_deleted = 0

), add_calculated_net_arr_to_opty_final AS (

    SELECT 
      oppty_final.*,
      
      COALESCE(oppty_final.opportunity_owner_user_segment ,'NA')                                                       AS sales_team_cro_level,
      COALESCE(CONCAT(oppty_final.opportunity_owner_user_segment,'_',oppty_final.opportunity_owner_user_region),'NA')  AS sales_team_rd_asm_level,

      ---------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------
      -- I am faking that using the upper CTE, that should be replaced by the official table
      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)         AS segment_order_type_iacv_to_net_arr_ratio,

      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE 
        WHEN oppty_final.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(oppty_final.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN oppty_final.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(oppty_final.net_incremental_acv,0) = 0
           THEN COALESCE(oppty_final.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN oppty_final.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(oppty_final.net_incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,

      -- Calculated NET ARR is only used for deals closed earlier than FY19 and that have no raw_net_arr
      CASE
        WHEN oppty_final.close_date < '2018-02-01'::DATE 
              AND COALESCE(oppty_final.raw_net_arr,0) = 0 
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(oppty_final.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,

      ---------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------
      -- current deal size field, it was creasted by the data team and the original doesn't work
      CASE 
        WHEN net_arr > 0 AND net_arr < 5000 
          THEN '1 - Small (<5k)'
        WHEN net_arr >=5000 AND net_arr < 25000 
          THEN '2 - Medium (5k - 25k)'
        WHEN net_arr >=25000 AND net_arr < 100000 
          THEN '3 - Big (25k - 100k)'
        WHEN net_arr >= 100000 
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

            -- calculated age field
      -- if open, use the diff between created date and snapshot date
      -- if closed, a) the close date is later than snapshot date, use snapshot date
      -- if closed, b) the close is in the past, use close date
      CASE
        WHEN oppty_final.is_open = 1
          THEN DATEDIFF(days, oppty_final.created_date, CURRENT_DATE)
        ELSE DATEDIFF(days, oppty_final.created_date, oppty_final.close_date)
      END                                                           AS calculated_age_in_days,

      -- Open pipeline eligibility definition
      CASE 
        WHEN oppty_final.deal_group IN ('1. New','2. Growth')
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.is_stage_1_plus = 1
          AND oppty_final.forecast_category_name != 'Omitted'
          AND oppty_final.is_open = 1
            THEN 1
        ELSE 0
      END                                                          AS is_eligible_open_pipeline_flag,


      -- Created pipeline eligibility definition
      CASE 
        WHEN oppty_final.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND oppty_final.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')  
          AND ((oppty_final.is_stage_1_plus = 1
                AND oppty_final.forecast_category_name != 'Omitted')
            OR oppty_final.is_lost = 1)
          AND (net_arr > 0 
            OR oppty_final.opportunity_category = 'Credit')
          -- 20210802 remove webpurchase deals
          AND oppty_final.is_web_portal_purchase = 0
         THEN 1
         ELSE 0
      END                                                          AS is_eligible_created_pipeline_flag,


      -- SAO alignment issue: https://mail.google.com/mail/u/0/#inbox/FMfcgzGkbDZKFplMhHCSFkPJSvDkTvCL
      CASE
        WHEN oppty_final.sales_accepted_date IS NOT NULL
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
            THEN 1
        ELSE 0
      END                                                         AS is_eligible_sao_flag,


      CASE 
        WHEN oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND oppty_final.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND oppty_final.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with net ARR < 0
          AND net_arr > 0
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_asp_analysis_flag,

      CASE 
        WHEN oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          -- Renewals are not having the same motion as rest of deals
          AND oppty_final.is_renewal = 0
          -- For stage age we exclude only ps/other
          AND oppty_final.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          -- Only include deal types with meaningful journeys through the stages
          AND oppty_final.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          -- Web Purchase have a different dynamic and should not be included
          AND oppty_final.is_web_portal_purchase = 0
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_age_analysis_flag,

      CASE
        WHEN oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          AND (oppty_final.is_won = 1 
              OR (oppty_final.is_renewal = 1 AND oppty_final.is_lost = 1))
          AND oppty_final.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_net_arr_flag,

      CASE
        WHEN oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          AND oppty_final.order_type_stamped IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_churn_contraction_flag,

      -- compound metrics to facilitate reporting
      -- created and closed within the quarter net arr
      CASE 
        WHEN oppty_final.pipeline_created_fiscal_quarter_date = oppty_final.close_fiscal_quarter_date
          AND is_eligible_created_pipeline_flag = 1
            THEN net_arr
        ELSE 0
      END                                                         AS created_and_won_same_quarter_net_arr,
  
      ---------------------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------------------
      -- Fields created to simplify report building down the road. Specially the pipeline velocity.

      -- deal count
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND oppty_final.is_stage_1_plus = 1
          THEN oppty_final.calculated_deal_count  
        ELSE 0                                                                                              
      END                                               AS open_1plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
         AND oppty_final.is_stage_3_plus = 1
          THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND oppty_final.is_stage_4_plus = 1
          THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,

      -- booked deal count
      CASE 
        WHEN oppty_final.is_won = 1
          THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,

      -- churned contraction deal count as OT
      CASE
        WHEN ((oppty_final.is_renewal = 1
            AND oppty_final.is_lost = 1)
            OR oppty_final.is_won = 1 )
            AND oppty_final.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                                 AS churned_contraction_deal_count,
    
      -----------------
      -- Net ARR

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          THEN net_arr
        ELSE 0                                                                                              
      END                                                AS open_1plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline_flag = 1
          AND oppty_final.is_stage_3_plus = 1   
          THEN net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,
  
      CASE 
        WHEN is_eligible_open_pipeline_flag = 1  
          AND oppty_final.is_stage_4_plus = 1
          THEN net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN (oppty_final.is_won = 1 
            OR (oppty_final.is_renewal = 1 
                AND oppty_final.is_lost = 1))
          THEN net_arr
        ELSE 0 
      END                                                 AS booked_net_arr,

      -- churned contraction net arr as OT
      CASE
        WHEN ((oppty_final.is_renewal = 1
            AND oppty_final.is_lost = 1)
            OR oppty_final.is_won = 1 )
            AND oppty_final.order_type_stamped IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,


      CASE 
        WHEN net_arr > -5000 
          THEN '1. < 5k'
        WHEN net_arr > -20000 AND net_arr <= -5000 
          THEN '2. 5k-20k'
        WHEN net_arr > -50000 AND net_arr <= -20000 
          THEN '3. 20k-50k'
        WHEN net_arr > -100000 AND net_arr <= -50000 
          THEN '4. 50k-100k'
        WHEN net_arr < -100000 
          THEN '5. 100k+'
      END                                                 AS churn_contracton_net_arr_bucket
      
    FROM oppty_final
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = oppty_final.opportunity_owner_user_segment
      AND net_iacv_to_net_arr_ratio.order_type_stamped = oppty_final.order_type_stamped

)
SELECT *
FROM add_calculated_net_arr_to_opty_final
