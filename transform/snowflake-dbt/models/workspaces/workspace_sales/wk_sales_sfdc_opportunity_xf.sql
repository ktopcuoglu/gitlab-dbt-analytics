{{ config(alias='sfdc_opportunity_xf') }}

WITH sfdc_opportunity AS (

    SELECT opportunity_id,
          opportunity_category
    FROM {{ref('sfdc_opportunity')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('wk_sales_sfdc_users_xf')}}

), sfdc_accounts_xf AS (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), date_details AS (
 
    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

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
      sfdc_opportunity_xf.opportunity_owner,
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
      sfdc_opportunity_xf.deal_path,
      sfdc_opportunity_xf.acv,
      sfdc_opportunity_xf.amount,
      sfdc_opportunity_xf.closed_deals,
      sfdc_opportunity_xf.competitors,
      sfdc_opportunity_xf.critical_deal_flag,
      sfdc_opportunity_xf.deal_size,
      sfdc_opportunity_xf.forecast_category_name,
      sfdc_opportunity_xf.forecasted_iacv,
      sfdc_opportunity_xf.incremental_acv,
      --sfdc_opportunity_xf.pre_covid_iacv,
      sfdc_opportunity_xf.invoice_number,

      -- logic needs to be added here once the oppotunity category fields is merged
      -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Credit', 'Decommission','Decommissioned')
          THEN 1
        ELSE 0
      END                                                          AS is_refund,
      --sfdc_opportunity_xf.is_refund,


      sfdc_opportunity_xf.is_downgrade,
      --sfdc_opportunity_xf.is_swing_deal,
      sfdc_opportunity_xf.is_edu_oss,
      sfdc_opportunity_xf.is_won,
      sfdc_opportunity_xf.net_incremental_acv,
      --sfdc_opportunity_xf.probability,
      sfdc_opportunity_xf.professional_services_value,
      --sfdc_opportunity_xf.pushed_count,
      sfdc_opportunity_xf.reason_for_loss,
      sfdc_opportunity_xf.reason_for_loss_details,
      --sfdc_opportunity_xf.refund_iacv,
      --sfdc_opportunity_xf.downgrade_iacv,
      sfdc_opportunity_xf.renewal_acv,
      sfdc_opportunity_xf.renewal_amount,
      sfdc_opportunity_xf.sales_qualified_source,
      sfdc_opportunity_xf.solutions_to_be_replaced,
      sfdc_opportunity_xf.total_contract_value,
      sfdc_opportunity_xf.upside_iacv,
      --sfdc_opportunity_xf.upside_swing_deal_iacv,
      --sfdc_opportunity_xf.weighted_iacv,
      sfdc_opportunity_xf.is_web_portal_purchase,
      sfdc_opportunity_xf.subscription_start_date,
      sfdc_opportunity_xf.subscription_end_date,
      --sfdc_opportunity_xf.true_up_value,

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

      sfdc_opportunity_xf.dr_partner_deal_type,
      sfdc_opportunity_xf.dr_partner_engagement,
      sfdc_opportunity_xf.partner_account,
      sfdc_opportunity_xf.dr_status,
      sfdc_opportunity_xf.distributor,
      sfdc_opportunity_xf.influence_partner,
      sfdc_opportunity_xf.fulfillment_partner,
      sfdc_opportunity_xf.platform_partner,
      sfdc_opportunity_xf.partner_track,
      sfdc_opportunity_xf.is_public_sector_opp,
      sfdc_opportunity_xf.is_registration_from_portal,
      sfdc_opportunity_xf.calculated_discount,
      sfdc_opportunity_xf.partner_discount,
      sfdc_opportunity_xf.partner_discount_calc,
      sfdc_opportunity_xf.comp_channel_neutral,
     
      -- NF: I think this one is deprecated too
      --sfdc_opportunity_xf.partner_initiated_opportunity,
      
      -----------------------------------------------------------
      -----------------------------------------------------------
      -- role hierarchy team fields, from the opportunity owner and account owner
      -- NF: 2020-02-18 TO BE REMOVED
      sfdc_opportunity_xf.account_owner_team_level_2,
      sfdc_opportunity_xf.account_owner_team_level_3,
      sfdc_opportunity_xf.account_owner_team_level_4,
      sfdc_opportunity_xf.account_owner_team_vp_level,
      sfdc_opportunity_xf.account_owner_team_rd_level,
      sfdc_opportunity_xf.account_owner_team_asm_level,
      sfdc_opportunity_xf.account_owner_min_team_level,
      sfdc_opportunity_xf.account_owner_sales_region,
      sfdc_opportunity_xf.opportunity_owner_team_level_2,
      sfdc_opportunity_xf.opportunity_owner_team_level_3,
      
      -----------------------------------------------------------
      -----------------------------------------------------------
      
      sfdc_opportunity_xf.stage_name_3plus,
      sfdc_opportunity_xf.stage_name_4plus,
      sfdc_opportunity_xf.is_stage_3_plus,
      sfdc_opportunity_xf.is_lost,
      
      --sfdc_opportunity_xf.is_open,
      -- NF: Added the 'Duplicate' stage to the is_open definition
      CASE 
        WHEN sfdc_opportunity_xf.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1  
      END                                                         AS is_open,
      
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

      sfdc_opportunity_xf.iacv_created_fiscal_quarter_name,
      sfdc_opportunity_xf.iacv_created_fiscal_quarter_date,
      sfdc_opportunity_xf.iacv_created_fiscal_year,
      sfdc_opportunity_xf.iacv_created_date_month,
      sfdc_opportunity_xf.iacv_created_date,

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
          ELSE COALESCE(sfdc_opportunity_xf.user_segment_stamped,'N/A')
      END                                                                    AS opportunity_owner_user_segment,

      --  stamped field is not maintained for open deals
      CASE WHEN sfdc_opportunity_xf.user_region_stamped IS NULL 
          THEN opportunity_owner.user_region
          ELSE COALESCE(sfdc_opportunity_xf.user_region_stamped,'N/A')
      END                                                                    AS opportunity_owner_user_region,

      -----------------------------------------------------------------------------------------------------      
      -----------------------------------------------------------------------------------------------------

      -- fields form opportunity source
      sfdc_opportunity.opportunity_category
    
    FROM {{ref('sfdc_opportunity_xf')}}
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

), oppty_final AS (

    SELECT 

      sfdc_opportunity_xf.*,

      -- date helpers
      
      -- pipeline created, tracks the date pipeline value was created for the first time
      -- used for performance reporting on pipeline generation
      -- these fields might change, isolating the field from the purpose
      -- alternatives are a future net_arr_created_date
      --sfdc_opportunity_xf.stage_1_date                             AS pipeline_created_date,
      --sfdc_opportunity_xf.stage_1_date_month                       AS pipeline_created_date_month,
      --sfdc_opportunity_xf.stage_1_fiscal_year                      AS pipeline_created_fiscal_year,
      --sfdc_opportunity_xf.stage_1_fiscal_quarter_name              AS pipeline_created_fiscal_quarter_name,
      --sfdc_opportunity_xf.stage_1_fiscal_quarter_date              AS pipeline_created_fiscal_quarter_date,

      --sfdc_opportunity_xf.net_arr_created_date                        AS pipeline_created_date,
      --sfdc_opportunity_xf.net_arr_created_date_month                  AS pipeline_created_date_month,
      --sfdc_opportunity_xf.net_arr_created_fiscal_year                 AS pipeline_created_fiscal_year,
      --sfdc_opportunity_xf.net_arr_created_fiscal_quarter_name         AS pipeline_created_fiscal_quarter_name,
      --sfdc_opportunity_xf.net_arr_created_fiscal_quarter_date         AS pipeline_created_fiscal_quarter_date,

      sfdc_opportunity_xf.created_date                        AS pipeline_created_date,
      sfdc_opportunity_xf.created_date_month                  AS pipeline_created_date_month,
      sfdc_opportunity_xf.created_fiscal_year                 AS pipeline_created_fiscal_year,
      sfdc_opportunity_xf.created_fiscal_quarter_name         AS pipeline_created_fiscal_quarter_name,
      sfdc_opportunity_xf.created_fiscal_quarter_date         AS pipeline_created_fiscal_quarter_date,

      /*
      COALESCE(sfdc_opportunity_xf.stage_1_date
              ,sfdc_opportunity_xf.net_arr_created_date
              ,sfdc_opportunity_xf.created_date::DATE)                        AS pipeline_created_date,  

      COALESCE(sfdc_opportunity_xf.stage_1_date_month
              ,sfdc_opportunity_xf.net_arr_created_date_month
              ,sfdc_opportunity_xf.created_date_month::DATE)                  AS pipeline_created_date_month, 

      COALESCE(sfdc_opportunity_xf.stage_1_fiscal_year
              ,sfdc_opportunity_xf.net_arr_created_fiscal_year
              ,sfdc_opportunity_xf.created_fiscal_year)                       AS pipeline_created_fiscal_year, 

      COALESCE(sfdc_opportunity_xf.stage_1_fiscal_quarter_name
              ,sfdc_opportunity_xf.net_arr_created_fiscal_quarter_name
              ,sfdc_opportunity_xf.created_fiscal_quarter_name)                AS pipeline_created_fiscal_quarter_name, 

      COALESCE(sfdc_opportunity_xf.stage_1_fiscal_quarter_date
              ,sfdc_opportunity_xf.net_arr_created_fiscal_quarter_date
              ,sfdc_opportunity_xf.created_fiscal_quarter_date)                AS pipeline_created_fiscal_quarter_date, 
      
      CASE
        WHEN sfdc_opportunity_xf.stage_1_date IS NOT NULL 
          THEN 'Stage 1 Date'
        WHEN sfdc_opportunity_xf.net_arr_created_date IS NOT NULL 
          THEN 'Net ARR Created Date'
        WHEN sfdc_opportunity_xf.created_date IS NOT NULL 
          THEN 'Opty Created Date'
      END                                                             AS pipeline_created_date_source,
      */

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
      sfdc_accounts_xf.ultimate_parent_account_id,
  
      -- medium level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('4. Churn','4. Contraction','6. Churn - Final')
          THEN '3. Churn'
        ELSE '4. Other' 
      END                                                                   AS deal_category,

      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Churn','4. Contraction','6. Churn - Final') 
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
      END                                                                     AS account_owner_team_stamped_cro_level,   

      ----------------------------------------------------------------
      ----------------------------------------------------------------

      -- fields for counting new logos, these fields count refund as negative
      CASE 
        WHEN sfdc_opportunity_xf.is_refund = 1
          THEN -1
        ELSE 1
      END                                                                      AS calculated_deal_count,

        -- PIO Flag for PIO reporting dashboard
      CASE 
        WHEN sfdc_opportunity_xf.dr_partner_engagement = 'PIO' 
          THEN 1 
        ELSE 0 
      END                                                                       AS partner_engaged_opportunity_flag,

      
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
        ELSE 0
      END                                                                       AS is_excluded_flag

    FROM sfdc_opportunity_xf
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_accounts_xf.account_id = sfdc_opportunity_xf.account_id
    
    WHERE sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')   -- remove test account
      AND sfdc_opportunity_xf.account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
      AND sfdc_opportunity_xf.is_deleted = 0

), add_calculated_net_arr_to_opty_final AS (

    SELECT 
      oppty_final.*,
      
      oppty_final.opportunity_owner_user_segment                                                        AS sales_team_cro_level,
      CONCAT(oppty_final.opportunity_owner_user_segment,'_',oppty_final.opportunity_owner_user_region)  AS sales_team_rd_asm_level,

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

      -- compound metrics to facilitate reporting
      -- created and closed within the quarter net arr
      CASE 
        WHEN oppty_final.created_fiscal_quarter_date = oppty_final.close_fiscal_quarter_date
          AND (oppty_final.is_won = 1 
                OR (oppty_final.is_renewal = 1 AND oppty_final.is_lost = 1)) 
            THEN net_arr
        ELSE 0
      END                                                         AS created_and_won_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN oppty_final.is_won = 1 OR (oppty_final.is_renewal = 1 AND oppty_final.is_lost = 1)
          THEN net_arr
        ELSE 0 
      END                                                         AS booked_net_arr

    FROM oppty_final
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = oppty_final.opportunity_owner_user_segment
      AND net_iacv_to_net_arr_ratio.order_type_stamped = oppty_final.order_type_stamped

)
SELECT *
FROM add_calculated_net_arr_to_opty_final
