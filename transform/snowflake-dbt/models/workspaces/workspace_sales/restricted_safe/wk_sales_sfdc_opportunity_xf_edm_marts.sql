{{ config(alias='sfdc_opportunity_xf_edm_marts') }}

WITH sfdc_opportunity AS (

    SELECT opportunity_id,
          opportunity_category,
          product_category
    FROM {{ref('sfdc_opportunity')}}
    --FROM  prod.restricted_safe_legacy.sfdc_opportunity

), legacy_sfdc_opportunity_xf AS (

    SELECT *
    --FROM prod.restricted_safe_legacy.sfdc_opportunity_xf
    FROM {{ref('sfdc_opportunity_xf')}}

), edm_opty AS (

    SELECT *
    --FROM prod.restricted_safe_common_mart_sales.mart_crm_opportunity
    FROM {{ref('mart_crm_opportunity')}}

), sfdc_users_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}}
    --FROM prod.restricted_safe_legacy.sfdc_accounts_xf

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), agg_demo_keys AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }}
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM date_details
  WHERE date_actual = CURRENT_DATE

-- CTE to be UPDATED using EDM fields as source
), sfdc_opportunity_xf AS (

   SELECT
    edm_opty.dbt_updated_at                   AS _last_dbt_run,
    edm_opty.dim_crm_account_id               AS account_id,
    edm_opty.dim_crm_opportunity_id           AS opportunity_id,
    edm_opty.opportunity_name                 AS opportunity_name,

    edm_opty.close_date                       AS close_date,
    edm_opty.created_date                     AS created_date,
    edm_opty.sales_accepted_date,
    edm_opty.sales_qualified_date,
    edm_opty.subscription_start_date          AS quote_start_date,
    edm_opty.subscription_end_date            AS quote_end_date,

    edm_opty.days_in_stage,
    edm_opty.deployment_preference,
    edm_opty.merged_crm_opportunity_id,
    ----------------------------------------------------------
    ----------------------------------------------------------
    --edm_opty.dim_crm_user_id                          AS owner_id,
    edm_opty.owner_id,
    edm_opty.opportunity_owner,
    edm_opty.opportunity_owner_department,
    edm_opty.opportunity_owner_manager,
    edm_opty.opportunity_owner_role,
    edm_opty.opportunity_owner_title,
    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.opportunity_term,
    edm_opty.primary_campaign_source_id            AS primary_campaign_source_id,
    edm_opty.sales_path                            AS sales_path,
    edm_opty.sales_type                            AS sales_type,
    edm_opty.stage_name                            AS stage_name,
    edm_opty.order_type                            AS order_type_stamped,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Amount fields
    COALESCE(edm_opty.net_arr,0)       AS raw_net_arr,
    edm_opty.net_arr,
    edm_opty.amount,
    edm_opty.renewal_amount,
    edm_opty.recurring_amount,
    edm_opty.true_up_amount,
    edm_opty.proserv_amount,
    edm_opty.other_non_recurring_amount,
    edm_opty.arr_basis,
    edm_opty.arr,

    edm_opty.competitors,
    edm_opty.fpa_master_bookings_flag,
    edm_opty.forecast_category_name,
    edm_opty.invoice_number,
    edm_opty.professional_services_value,
    edm_opty.reason_for_loss,
    edm_opty.reason_for_loss_details,
    edm_opty.downgrade_reason,

    edm_opty.is_downgrade,
    edm_opty.is_edu_oss,
    edm_opty.solutions_to_be_replaced,
    edm_opty.total_contract_value,
    edm_opty.is_web_portal_purchase,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Support Team Members
    edm_opty.opportunity_business_development_representative,
    edm_opty.opportunity_sales_development_representative,
    edm_opty.opportunity_development_representative,
    -- Missing ISR & TAM

    ----------------------------------------------------------
    ----------------------------------------------------------

    edm_opty.opportunity_health,
    edm_opty.is_risky,
    edm_opty.risk_type,
    edm_opty.risk_reasons,
    edm_opty.tam_notes,
    edm_opty.days_in_1_discovery,
    edm_opty.days_in_2_scoping,
    edm_opty.days_in_3_technical_evaluation,
    edm_opty.days_in_4_proposal,
    edm_opty.days_in_5_negotiating,
    edm_opty.stage_0_pending_acceptance_date,
    edm_opty.stage_1_discovery_date,
    edm_opty.stage_2_scoping_date,
    edm_opty.stage_3_technical_evaluation_date,
    edm_opty.stage_4_proposal_date,
    edm_opty.stage_5_negotiating_date,
    
    sfdc_opportunity_xf.stage_6_awaiting_signature_date,
    
    edm_opty.stage_6_closed_won_date,
    edm_opty.stage_6_closed_lost_date,
    edm_opty.cp_champion,
    edm_opty.cp_close_plan,
    edm_opty.cp_competition,
    edm_opty.cp_decision_criteria,
    edm_opty.cp_decision_process,
    edm_opty.cp_economic_buyer,
    edm_opty.cp_identify_pain,
    edm_opty.cp_metrics,
    edm_opty.cp_risks,
    edm_opty.cp_use_cases,
    edm_opty.cp_value_driver,
    edm_opty.cp_why_do_anything_at_all,
    edm_opty.cp_why_gitlab,
    edm_opty.cp_why_now,
    edm_opty.cp_score,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- fields form opportunity source
    edm_opty.opportunity_category,
    edm_opty.product_category,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- NF: why do we need these fields now?
    sfdc_opportunity_xf.sales_segment, -- drop?

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Channel Org. fields
    edm_opty.deal_path_name                             AS deal_path,
    edm_opty.dr_partner_deal_type,
    edm_opty.dr_partner_engagement_name                 AS dr_partner_engagement, 
    edm_opty.partner_account                            AS partner_account_id,
    edm_opty.partner_account_name,
    edm_opty.dr_status,
    edm_opty.distributor,
    edm_opty.influence_partner,

    edm_opty.partner_track,
    edm_opty.partner_gitlab_program,

    edm_opty.is_public_sector_opp,
    edm_opty.is_registration_from_portal,
    edm_opty.calculated_discount,
    edm_opty.partner_discount,
    edm_opty.partner_discount_calc,
    edm_opty.comp_channel_neutral,
    edm_opty.fulfillment_partner                      AS resale_partner_id,
    
    resale_account.account_name                       AS resale_partner_name,
    
    edm_opty.platform_partner                         AS platform_partner_id,

    ----------------------------------------------------------
    ----------------------------------------------------------

    -- account driven fields
    edm_opty.crm_account_name                         AS account_name,
    edm_opty.dim_parent_crm_account_id                AS ultimate_parent_account_id,
    edm_opty.is_jihu_account,

    edm_opty.account_owner_user_segment,
    edm_opty.account_owner_user_geo,
    edm_opty.account_owner_user_region,
    edm_opty.account_owner_user_area,

    edm_opty.account_demographics_segment,
    edm_opty.account_demographics_geo,
    edm_opty.account_demographics_region,
    edm_opty.account_demographics_area,
    edm_opty.account_demographics_territory,

    edm_opty.account_demographics_segment             AS upa_demographics_segment,
    edm_opty.account_demographics_geo                 AS upa_demographics_geo,
    edm_opty.account_demographics_region              AS upa_demographics_region,
    edm_opty.account_demographics_area                AS upa_demographics_area,
    edm_opty.account_demographics_territory           AS upa_demographics_territory,
    
    ----------------------------------------------------------
    ----------------------------------------------------------

    edm_opty.sales_qualified_source_name             AS sales_qualified_source,
    edm_opty.stage_category,
    edm_opty.calculated_partner_track,
    edm_opty.deal_path_engagement,
    edm_opty.is_refund,
    edm_opty.is_credit                               AS is_credit_flag,
    edm_opty.is_contract_reset                       AS is_contract_reset_flag,
    CAST(edm_opty.is_won AS INTEGER)                 AS is_won,
    edm_opty.is_lost,
    edm_opty.is_open,
    edm_opty.is_duplicate                            AS is_duplicate_flag,
    edm_opty.is_closed,
    edm_opty.is_renewal,


    -- date fields helpers -- revisit
    edm_opty.close_fiscal_quarter_name,
    edm_opty.close_fiscal_quarter_date,
    edm_opty.close_fiscal_year,
    edm_opty.close_month                                                 AS close_date_month,

    edm_opty.created_fiscal_quarter_name,
    edm_opty.created_fiscal_quarter_date,
    edm_opty.created_fiscal_year,
    edm_opty.created_month                                               AS created_date_month,

    edm_opty.subscription_start_fiscal_quarter_name                      AS quote_start_date_fiscal_quarter_name,
    edm_opty.subscription_start_fiscal_quarter_date                      AS quote_start_date_fiscal_quarter_date,
    edm_opty.subscription_start_fiscal_year                              AS quote_start_date_fiscal_year,
    edm_opty.subscription_start_month                                    AS quote_start_date_month,

    edm_opty.sales_accepted_fiscal_quarter_name,
    edm_opty.sales_accepted_fiscal_quarter_date,
    edm_opty.sales_accepted_fiscal_year,
    edm_opty.sales_accepted_month                                        AS sales_accepted_date_month,

    edm_opty.sales_qualified_fiscal_quarter_name,
    edm_opty.sales_qualified_fiscal_quarter_date,
    edm_opty.sales_qualified_fiscal_year,
    edm_opty.sales_qualified_month                                       AS sales_qualified_date_month,

    edm_opty.net_arr_created_date,
    edm_opty.net_arr_created_fiscal_quarter_name,
    edm_opty.net_arr_created_fiscal_quarter_date,
    edm_opty.net_arr_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS net_arr_created_date_month,

    edm_opty.pipeline_created_date,
    edm_opty.pipeline_created_fiscal_quarter_name,
    edm_opty.pipeline_created_fiscal_quarter_date,
    edm_opty.pipeline_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS pipeline_created_date_month,

    edm_opty.stage_1_discovery_date                                      AS stage_1_date,
    edm_opty.stage_1_discovery_month                                     AS stage_1_date_month,
    edm_opty.stage_1_discovery_fiscal_year                               AS stage_1_fiscal_year,
    edm_opty.stage_1_discovery_fiscal_quarter_name                       AS stage_1_fiscal_quarter_name,
    edm_opty.stage_1_discovery_fiscal_quarter_date                       AS stage_1_fiscal_quarter_date,

    edm_opty.stage_3_technical_evaluation_date                           AS stage_3_date,
    edm_opty.stage_3_technical_evaluation_month                          AS stage_3_date_month,
    edm_opty.stage_3_technical_evaluation_fiscal_year                    AS stage_3_fiscal_year,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_name            AS stage_3_fiscal_quarter_name,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_date            AS stage_3_fiscal_quarter_date,

    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    -- Opportunity User fields
    -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields

    -- Team Segment / ASM - RD
    -- NF 2022-01-28 Data seems clean in SFDC, but leaving the fallback just in case
    -- NF 2022-04-27 There are issues with the stamped field not reflecting the real owner of the opportunity
    --                adding is_open check here to default open deals to opportunity owners fields (instead of stamped)
    
    edm_opty.opportunity_owner_user_segment,
    edm_opty.opportunity_owner_user_geo,
    edm_opty.opportunity_owner_user_region,
    edm_opty.opportunity_owner_user_area,
    edm_opty.competitors_other_flag,
    edm_opty.competitors_gitlab_core_flag,
    edm_opty.competitors_none_flag,
    edm_opty.competitors_github_enterprise_flag,
    edm_opty.competitors_bitbucket_server_flag,
    edm_opty.competitors_unknown_flag,
    edm_opty.competitors_github_flag,
    edm_opty.competitors_gitlab_flag,
    edm_opty.competitors_jenkins_flag,
    edm_opty.competitors_azure_devops_flag,
    edm_opty.competitors_svn_flag,
    edm_opty.competitors_bitbucket_flag,
    edm_opty.competitors_atlassian_flag,
    edm_opty.competitors_perforce_flag,
    edm_opty.competitors_visual_studio_flag,
    edm_opty.competitors_azure_flag,
    edm_opty.competitors_amazon_code_commit_flag,
    edm_opty.competitors_circleci_flag,
    edm_opty.competitors_bamboo_flag,
    edm_opty.competitors_aws_flag,
    edm_opty.is_comp_new_logo_override,
    edm_opty.is_stage_1_plus,
    edm_opty.is_stage_3_plus,
    edm_opty.is_stage_4_plus,
    edm_opty.stage_name_3plus,
    edm_opty.stage_name_4plus,
    edm_opty.deal_category,
    edm_opty.deal_group,
    edm_opty.pipeline_calculated_deal_count                                  AS calculated_deal_count,

    ----------------------------------------------------------------
    -- NF 2022-01-28 This is probably TO BE DEPRECATED too, need to align with Channel ops
    -- PIO Flag for PIO reporting dashboard
    CASE
    WHEN edm_opty.dr_partner_engagement = 'PIO'
        THEN 1
    ELSE 0
    END                                                                      AS partner_engaged_opportunity_flag,

    -- check if renewal was closed on time or not
    CASE
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   >= edm_opty.close_fiscal_quarter_date
        THEN 'On-Time'
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   < edm_opty.close_fiscal_quarter_date
        THEN 'Late'
    END                                                                       AS renewal_timing_status,

    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- calculated fields for pipeline velocity report

    -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
    edm_opty.is_excluded_from_pipeline_created                                AS is_excluded_flag,

    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- NF 20220727 These next fields are needed for custom logic down the line
    -- sfdc_opportunity_xf.incremental_acv,
    -- sfdc_opportunity_xf.net_incremental_acv,
    sfdc_opportunity_xf.is_deleted,
    -----------------------------------------------

    edm_opty.report_opportunity_user_segment,
    edm_opty.report_opportunity_user_geo,
    edm_opty.report_opportunity_user_region,
    edm_opty.report_opportunity_user_area,
    
    -- NF 2022-02-17 these next two fields leverage the logic of comparing current fy opportunity demographics stamped vs account demo for previous years
    edm_opty.report_user_segment_geo_region_area,
    edm_opty.report_user_segment_geo_region_area_sqs_ot,

    ---- measures
    edm_opty.open_1plus_deal_count,
    edm_opty.open_3plus_deal_count,
    edm_opty.open_4plus_deal_count,
    edm_opty.booked_deal_count,
    edm_opty.churned_contraction_deal_count,
    edm_opty.booked_churned_contraction_deal_count,
    edm_opty.open_1plus_net_arr,
    edm_opty.open_3plus_net_arr,
    edm_opty.open_4plus_net_arr,
    edm_opty.booked_net_arr,
    edm_opty.booked_churned_contraction_net_arr,
    edm_opty.churned_contraction_net_arr,

    -- NF 2022-02-17 These keys are used in the pipeline metrics models and on the X-Ray dashboard to link gSheets with
    -- different aggregation levels
    edm_opty.key_sqs,
    edm_opty.key_ot,
    edm_opty.key_segment,
    edm_opty.key_segment_sqs,
    edm_opty.key_segment_ot,
    edm_opty.key_segment_geo,
    edm_opty.key_segment_geo_sqs,
    edm_opty.key_segment_geo_ot,
    edm_opty.key_segment_geo_region,
    edm_opty.key_segment_geo_region_sqs,
    edm_opty.key_segment_geo_region_ot,
    edm_opty.key_segment_geo_region_area,
    edm_opty.key_segment_geo_region_area_sqs,
    edm_opty.key_segment_geo_region_area_ot,
    edm_opty.key_segment_geo_area,
    edm_opty.sales_team_cro_level,
    edm_opty.sales_team_rd_asm_level,
    edm_opty.sales_team_vp_level,
    edm_opty.sales_team_avp_rd_level,
    edm_opty.sales_team_asm_level,

    edm_opty.deal_size,
    edm_opty.calculated_deal_size,
    edm_opty.calculated_age_in_days,
    edm_opty.is_eligible_open_pipeline              AS is_eligible_open_pipeline_flag,
    edm_opty.is_eligible_asp_analysis               AS is_eligible_asp_analysis_flag,
    edm_opty.is_eligible_age_analysis               AS is_eligible_age_analysis_flag,
    edm_opty.is_booked_net_arr                      AS is_booked_net_arr_flag,
    edm_opty.is_eligible_churn_contraction          AS is_eligible_churn_contraction_flag,
    edm_opty.created_and_won_same_quarter_net_arr,
    edm_opty.churn_contraction_net_arr_bucket       AS churn_contracton_net_arr_bucket,  --typo in wk sales
    edm_opty.reason_for_loss_calc


    FROM legacy_sfdc_opportunity_xf AS sfdc_opportunity_xf
    -- Date helpers
    INNER JOIN sfdc_accounts_xf AS account
      ON account.account_id = sfdc_opportunity_xf.account_id
    INNER JOIN date_details AS created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity_xf.created_date::DATE
        -- not all fields are in opportunity xf
    INNER JOIN sfdc_opportunity
      ON sfdc_opportunity.opportunity_id = sfdc_opportunity_xf.opportunity_id
    INNER JOIN sfdc_users_xf AS opportunity_owner
      ON opportunity_owner.user_id = sfdc_opportunity_xf.owner_id
    INNER JOIN edm_opty
      ON edm_opty.dim_crm_opportunity_id = sfdc_opportunity_xf.opportunity_id
    LEFT JOIN date_details AS start_date
      ON sfdc_opportunity_xf.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN sfdc_accounts_xf AS resale_account
      ON resale_account.account_id = sfdc_opportunity_xf.fulfillment_partner
    -- NF 20210906 remove JiHu opties from the models
    WHERE sfdc_opportunity_xf.is_jihu_account = 0
        AND account.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')            -- remove test account
        AND sfdc_opportunity_xf.account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
        AND sfdc_opportunity_xf.is_deleted = 0


), churn_metrics AS (

SELECT
    o.opportunity_id,
    NVL(o.reason_for_loss, o.downgrade_reason) AS reason_for_loss_staged,
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

      /*
      FY23 fields
      2022-01-28 NF

        There are different layers of reporting.
        Account Owner -> Used to report performance of territories year over year, they are comparable across years
          as it will be restated for all accounts after carving
        Opportunity Owner -> Used to report performance, the team might be different to the Account Owner due to holdovers
          (accounts kept by a Sales Rep for a certain amount of time)
        Account Demographics -> The fields that would be appropiate to that account according to their address, it might not match the one
          of the account owner
        Report -> This will be a calculated field, using Opportunity Owner for current fiscal year opties and Account for anything before
        Sales Team -> Same as report, but with a naming convention closer to the sales org hierarchy

      */

      
      -------------------
      -- BASE KEYS
      -- 20220214 NF: Temporary keys, until the SFDC key is exposed
      LOWER(CONCAT(sfdc_opportunity_xf.opportunity_owner_user_segment,'-',sfdc_opportunity_xf.opportunity_owner_user_geo,'-',sfdc_opportunity_xf.opportunity_owner_user_region,'-',sfdc_opportunity_xf.opportunity_owner_user_area)) AS opportunity_user_segment_geo_region_area,

      -- Customer Success related fields
      -- DRI Michael Armtz
      churn_metrics.reason_for_loss_staged,
      -- churn_metrics.reason_for_loss_calc, -- part of edm opp mart
      churn_metrics.churn_contraction_type_calc

    FROM sfdc_opportunity_xf
    CROSS JOIN today
   LEFT JOIN churn_metrics
      ON churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id

), add_calculated_net_arr_to_opty_final AS (

    SELECT
      oppty_final.*,

      -- Created pipeline eligibility definition
      -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
      CASE
        WHEN oppty_final.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND oppty_final.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
          -- 20211222 Adjusted to remove the ommitted filter
          AND oppty_final.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (net_arr > 0
            OR oppty_final.opportunity_category = 'Credit')
          -- 20220128 Updated to remove webdirect SQS deals
          AND oppty_final.sales_qualified_source  != 'Web Direct Generated'
          AND oppty_final.is_jihu_account = 0
         THEN 1
         ELSE 0
      END                                                          AS is_eligible_created_pipeline_flag,


      -- SAO alignment issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
      CASE
        WHEN oppty_final.sales_accepted_date IS NOT NULL
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          AND oppty_final.is_renewal = 0
          AND oppty_final.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
            THEN 1
        ELSE 0
      END                                                         AS is_eligible_sao_flag



    FROM oppty_final
    -- Add keys for aggregated analysis
    LEFT JOIN agg_demo_keys
      ON oppty_final.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot

)
SELECT *
FROM add_calculated_net_arr_to_opty_final
