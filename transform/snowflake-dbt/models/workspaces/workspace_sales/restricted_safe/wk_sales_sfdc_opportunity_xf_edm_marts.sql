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
    -- TODO add the reference

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

    opportunity_owner.name                          AS opportunity_owner,
    opportunity_owner.department                    AS opportunity_owner_department,
    opportunity_owner.manager_name                  AS opportunity_owner_manager,
    opportunity_owner.role_name                     AS opportunity_owner_role,
    opportunity_owner.title                         AS opportunity_owner_title,
    ----------------------------------------------------------
    ----------------------------------------------------------
    sfdc_opportunity_xf.opportunity_term,
    edm_opty.primary_campaign_source_id            AS primary_campaign_source_id,
    edm_opty.sales_path                            AS sales_path,
    edm_opty.sales_type                            AS sales_type,
    edm_opty.stage_name                            AS stage_name,
    edm_opty.order_type                            AS order_type_stamped,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Amount fields

    COALESCE(edm_opty.net_arr,0)       AS raw_net_arr,
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
    account.ultimate_parent_account_id,
    edm_opty.is_jihu_account,

    edm_opty.account_owner_user_segment,
    edm_opty.account_owner_user_geo,
    edm_opty.account_owner_user_region,
    edm_opty.account_owner_user_area,

    edm_opty.account_demographics_segment             AS account_demographics_sales_segment,
    edm_opty.account_demographics_geo,
    edm_opty.account_demographics_region,
    edm_opty.account_demographics_area,
    edm_opty.account_demographics_territory,

    account.upa_demographics_segment,
    account.upa_demographics_geo,
    account.upa_demographics_region,
    account.upa_demographics_area,
    account.upa_demographics_territory,

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

    created_date_detail.fiscal_quarter_name_fy                           AS created_fiscal_quarter_name,
    created_date_detail.first_day_of_fiscal_quarter                      AS created_fiscal_quarter_date,
    created_date_detail.fiscal_year                                      AS created_fiscal_year,
    created_date_detail.first_day_of_month                               AS created_date_month,

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
    edm_opty.net_arr_created_month                                       AS net_arr_created_date_month

    edm_opty.pipeline_created_date,
    edm_opty.pipeline_created_fiscal_quarter_name,
    edm_opty.pipeline_created_fiscal_quarter_date,
    edm_opty.pipeline_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS pipeline_created_date_month

    stage_1_date.date_actual                                             AS stage_1_date,
    stage_1_date.first_day_of_month                                      AS stage_1_date_month,
    stage_1_date.fiscal_year                                             AS stage_1_fiscal_year,
    stage_1_date.fiscal_quarter_name_fy                                  AS stage_1_fiscal_quarter_name,
    stage_1_date.first_day_of_fiscal_quarter                             AS stage_1_fiscal_quarter_date,

    stage_3_date.date_actual                                             AS stage_3_date,
    stage_3_date.first_day_of_month                                      AS stage_3_date_month,
    stage_3_date.fiscal_year                                             AS stage_3_fiscal_year,
    stage_3_date.fiscal_quarter_name_fy                                  AS stage_3_fiscal_quarter_name,
    stage_3_date.first_day_of_fiscal_quarter                             AS stage_3_fiscal_quarter_date,

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
    sfdc_opportunity_xf.incremental_acv,
    sfdc_opportunity_xf.net_incremental_acv,
    sfdc_opportunity_xf.is_deleted
    -----------------------------------------------


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
    --LEFT JOIN date_details AS iacv_created_date
    --  ON iacv_created_date.date_actual = sfdc_opportunity_xf.iacv_created_date::DATE
    -- pipeline creation date
    LEFT JOIN date_details AS stage_1_date
      ON stage_1_date.date_actual = sfdc_opportunity_xf.stage_1_discovery_date::date
    -- pipeline creation date
    LEFT JOIN date_details AS stage_3_date
      ON stage_3_date.date_actual = sfdc_opportunity_xf.stage_3_technical_evaluation_date::date
    -- NF 20211105 resale partner
    LEFT JOIN sfdc_accounts_xf AS resale_account
      ON resale_account.account_id = sfdc_opportunity_xf.fulfillment_partner
    -- NF 20210906 remove JiHu opties from the models
    WHERE sfdc_opportunity_xf.is_jihu_account = 0
        AND account.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')            -- remove test account
        AND sfdc_opportunity_xf.account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
        AND sfdc_opportunity_xf.is_deleted = 0

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

      CASE
        WHEN sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
          THEN sfdc_opportunity_xf.account_owner_user_segment
        ELSE sfdc_opportunity_xf.opportunity_owner_user_segment
      END                                                       AS report_opportunity_user_segment,

      CASE
        WHEN sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
          THEN sfdc_opportunity_xf.account_owner_user_geo
        ELSE sfdc_opportunity_xf.opportunity_owner_user_geo
      END                                                       AS report_opportunity_user_geo,

      CASE
        WHEN sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
          THEN sfdc_opportunity_xf.account_owner_user_region
        ELSE sfdc_opportunity_xf.opportunity_owner_user_region
      END                                                       AS report_opportunity_user_region,

      CASE
        WHEN sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
          THEN sfdc_opportunity_xf.account_owner_user_area
        ELSE sfdc_opportunity_xf.opportunity_owner_user_area
      END                                                       AS report_opportunity_user_area,
      -- report_opportunity_subarea

      -------------------
      -- BASE KEYS
       -- 20220214 NF: Temporary keys, until the SFDC key is exposed
      LOWER(CONCAT(sfdc_opportunity_xf.opportunity_owner_user_segment,'-',sfdc_opportunity_xf.opportunity_owner_user_geo,'-',sfdc_opportunity_xf.opportunity_owner_user_region,'-',sfdc_opportunity_xf.opportunity_owner_user_area)) AS opportunity_user_segment_geo_region_area,

      -- NF 2022-02-17 these next two fields leverage the logic of comparing current fy opportunity demographics stamped vs account demo for previous years
      LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area)) AS report_user_segment_geo_region_area,
      LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area, '-', sfdc_opportunity_xf.sales_qualified_source, '-', sfdc_opportunity_xf.order_type_stamped)) AS report_user_segment_geo_region_area_sqs_ot,

      -- Customer Success related fields
      -- DRI Michael Armtz
      churn_metrics.reason_for_loss_staged,
      churn_metrics.reason_for_loss_calc,
      churn_metrics.churn_contraction_type_calc

    FROM sfdc_opportunity_xf
    CROSS JOIN today
   LEFT JOIN churn_metrics
      ON churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id

), add_calculated_net_arr_to_opty_final AS (

    SELECT
      oppty_final.*,

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
      END                                                         AS is_eligible_sao_flag,

      -- ASP Analysis eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
      CASE
        WHEN oppty_final.is_edu_oss = 0
          AND oppty_final.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND oppty_final.order_type_stamped IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND oppty_final.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND net_arr > 0
            THEN 1
          ELSE 0
      END                                                           AS is_eligible_asp_analysis_flag,

      -- Age eligibility issue: https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
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
      END                                                           AS is_booked_net_arr_flag,

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
        WHEN is_eligible_churn_contraction_flag = 1
        THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                                 AS churned_contraction_deal_count,


        CASE
        WHEN ((oppty_final.is_renewal = 1
                AND oppty_final.is_lost = 1)
              OR oppty_final.is_won = 1 )
            AND is_eligible_churn_contraction_flag = 1
        THEN oppty_final.calculated_deal_count
        ELSE 0
      END                                                 AS booked_churned_contraction_deal_count,
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

      -- booked churned contraction net arr as OT
      CASE
        WHEN
          ((oppty_final.is_renewal = 1
            AND oppty_final.is_lost = 1)
            OR oppty_final.is_won = 1 )
            AND is_eligible_churn_contraction_flag = 1
        THEN net_arr
        ELSE 0
      END                                                 AS booked_churned_contraction_net_arr,

      -- churned contraction net arr as OT
      CASE
        WHEN is_eligible_churn_contraction_flag = 1
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,

      CASE
        WHEN net_arr > -5000
            AND is_eligible_churn_contraction_flag = 1
          THEN '1. < 5k'
        WHEN net_arr > -20000
          AND net_arr <= -5000
          AND is_eligible_churn_contraction_flag = 1
          THEN '2. 5k-20k'
        WHEN net_arr > -50000
          AND net_arr <= -20000
          AND is_eligible_churn_contraction_flag = 1
          THEN '3. 20k-50k'
        WHEN net_arr > -100000
          AND net_arr <= -50000
          AND is_eligible_churn_contraction_flag = 1
          THEN '4. 50k-100k'
        WHEN net_arr < -100000
          AND is_eligible_churn_contraction_flag = 1
          THEN '5. 100k+'
      END                                                 AS churn_contracton_net_arr_bucket,

      -- NF 2022-02-17 These keys are used in the pipeline metrics models and on the X-Ray dashboard to link gSheets with
      -- different aggregation levels

        COALESCE(agg_demo_keys.key_sqs,'other')                         AS key_sqs,
        COALESCE(agg_demo_keys.key_ot,'other')                          AS key_ot,

        COALESCE(agg_demo_keys.key_segment,'other')                     AS key_segment,
        COALESCE(agg_demo_keys.key_segment_sqs,'other')                 AS key_segment_sqs,
        COALESCE(agg_demo_keys.key_segment_ot,'other')                  AS key_segment_ot,

        COALESCE(agg_demo_keys.key_segment_geo,'other')                 AS key_segment_geo,
        COALESCE(agg_demo_keys.key_segment_geo_sqs,'other')             AS key_segment_geo_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_ot,'other')              AS key_segment_geo_ot,

        COALESCE(agg_demo_keys.key_segment_geo_region,'other')          AS key_segment_geo_region,
        COALESCE(agg_demo_keys.key_segment_geo_region_sqs,'other')      AS key_segment_geo_region_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_region_ot,'other')       AS key_segment_geo_region_ot,

        COALESCE(agg_demo_keys.key_segment_geo_region_area,'other')     AS key_segment_geo_region_area,
        COALESCE(agg_demo_keys.key_segment_geo_region_area_sqs,'other') AS key_segment_geo_region_area_sqs,
        COALESCE(agg_demo_keys.key_segment_geo_region_area_ot,'other')  AS key_segment_geo_region_area_ot,

        COALESCE(agg_demo_keys.key_segment_geo_area,'other')  AS key_segment_geo_area,

        COALESCE(agg_demo_keys.report_opportunity_user_segment ,'other')   AS sales_team_cro_level,

        -- NF: This code replicates the reporting structured of FY22, to keep current tools working
        COALESCE(agg_demo_keys.sales_team_rd_asm_level,'other')  AS sales_team_rd_asm_level,

        COALESCE(agg_demo_keys.sales_team_vp_level,'other')      AS sales_team_vp_level,
        COALESCE(agg_demo_keys.sales_team_avp_rd_level,'other')  AS sales_team_avp_rd_level,
        COALESCE(agg_demo_keys.sales_team_asm_level,'other')     AS sales_team_asm_level


    FROM oppty_final
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = oppty_final.opportunity_owner_user_segment
      AND net_iacv_to_net_arr_ratio.order_type_stamped = oppty_final.order_type_stamped
    -- Add keys for aggregated analysis
    LEFT JOIN agg_demo_keys
      ON oppty_final.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot

)
SELECT *
FROM add_calculated_net_arr_to_opty_final
