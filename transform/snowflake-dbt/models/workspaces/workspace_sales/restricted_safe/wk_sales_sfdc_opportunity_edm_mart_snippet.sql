{{ config(alias='sfdc_opportunity_edm_mart_snippet') }}

SELECT
    edm_opty.dbt_updated_at                   AS _last_dbt_run,
    edm_opty.dim_crm_account_id               AS account_id,
    edm_opty.dim_crm_opportunity_id           AS opportunity_id,
    edm_opty.opportunity_name                 AS opportunity_name,

    edm_opty.close_date                       AS close_date,
    edm_opty.created_date                     AS created_date,
    edm_opty.sales_accepted_date,
    sfdc_opportunity_xf.sales_qualified_date,
    sfdc_opportunity_xf.subscription_start_date  AS quote_start_date,
    sfdc_opportunity_xf.subscription_end_date    AS quote_end_date,

    sfdc_opportunity_xf.days_in_stage,
    sfdc_opportunity_xf.deployment_preference,
    sfdc_opportunity_xf.merged_opportunity_id,
    ----------------------------------------------------------
    ----------------------------------------------------------
    --edm_opty.dim_crm_user_id                          AS owner_id,
    sfdc_opportunity_xf.owner_id                        AS owner_id,

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

    COALESCE(sfdc_opportunity_xf.net_arr,0)       AS raw_net_arr,
    edm_opty.amount,
    sfdc_opportunity_xf.renewal_amount,
    sfdc_opportunity_xf.recurring_amount,
    sfdc_opportunity_xf.true_up_amount,
    sfdc_opportunity_xf.proserv_amount,
    sfdc_opportunity_xf.other_non_recurring_amount,
    edm_opty.arr_basis,
    sfdc_opportunity_xf.arr,

    sfdc_opportunity_xf.competitors,
    edm_opty.fpa_master_bookings_flag,
    sfdc_opportunity_xf.forecast_category_name,
    sfdc_opportunity_xf.invoice_number,
    sfdc_opportunity_xf.professional_services_value,
    sfdc_opportunity_xf.reason_for_loss,
    sfdc_opportunity_xf.reason_for_loss_details,
    sfdc_opportunity_xf.downgrade_reason,

    sfdc_opportunity_xf.is_downgrade,
    edm_opty.is_edu_oss,
    sfdc_opportunity_xf.solutions_to_be_replaced,
    sfdc_opportunity_xf.total_contract_value,
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
    sfdc_opportunity_xf.cp_score,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- fields form opportunity source
    edm_opty.opportunity_category,
    edm_opty.product_category,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- NF: why do we need these fields now?
    sfdc_opportunity_xf.sales_segment,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Channel Org. fields
    sfdc_opportunity_xf.deal_path,
    edm_opty.dr_partner_deal_type,
    sfdc_opportunity_xf.dr_partner_engagement,
    edm_opty.partner_account                            AS partner_account_id,
    partner_account.account_name                        AS partner_account_name,
    edm_opty.dr_status,
    edm_opty.distributor,
    edm_opty.influence_partner,

    edm_opty.partner_track                             AS partner_track,
    partner_account.gitlab_partner_program             AS partner_gitlab_program,

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
    account.account_name,
    account.ultimate_parent_account_id,
    account.is_jihu_account,

    account.account_owner_user_segment,
    account.account_owner_user_geo,
    account.account_owner_user_region,
    account.account_owner_user_area,

    account.account_demographics_sales_segment,
    account.account_demographics_geo,
    account.account_demographics_region,
    account.account_demographics_area,
    account.account_demographics_territory,

    account.upa_demographics_segment,
    account.upa_demographics_geo,
    account.upa_demographics_region,
    account.upa_demographics_area,
    account.upa_demographics_territory,

    ----------------------------------------------------------
    ----------------------------------------------------------

    CASE
    WHEN edm_opty.sales_qualified_source_name = 'BDR Generated'
        THEN 'SDR Generated'
    ELSE COALESCE(edm_opty.sales_qualified_source_name,'NA')
    END                                                           AS sales_qualified_source,

    CASE
    WHEN edm_opty.is_won = 1 THEN '1.Won'
    WHEN edm_opty.stage_name = '8-Closed Lost'
        THEN '2.Lost'
    WHEN edm_opty.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN '0. Open'
    ELSE 'N/A'
    END                                                        AS stage_category,

    CASE
    WHEN sfdc_opportunity_xf.deal_path = 'Channel'
        THEN REPLACE(COALESCE(sfdc_opportunity_xf.partner_track,partner_account.partner_track, resale_account.partner_track,'Open'),'select','Select')
    ELSE 'Direct'
    END                                                         AS calculated_partner_track,

    CASE
    WHEN sfdc_opportunity_xf.deal_path = 'Direct'
        THEN 'Direct'
    WHEN sfdc_opportunity_xf.deal_path = 'Web Direct'
        THEN 'Web Direct'
    WHEN sfdc_opportunity_xf.deal_path = 'Channel'
        AND sfdc_opportunity_xf.sales_qualified_source = 'Channel Generated'
        THEN 'Partner Sourced'
    WHEN sfdc_opportunity_xf.deal_path = 'Channel'
        AND edm_opty.sales_qualified_source_name != 'Channel Generated'
        THEN 'Partner Co-Sell'
    END                                                         AS deal_path_engagement,

    CASE
    WHEN edm_opty.opportunity_category IN ('Decommission')
        THEN 1
    ELSE 0
    END                                                         AS is_refund,

    CASE
    WHEN edm_opty.opportunity_category IN ('Credit')
        THEN 1
    ELSE 0
    END                                                         AS is_credit_flag,

    CASE
    WHEN edm_opty.opportunity_category IN ('Contract Reset')
        THEN 1
    ELSE 0
    END                                                         AS is_contract_reset_flag,


    CAST(edm_opty.is_won AS INTEGER)                 AS is_won,

    CASE
    WHEN edm_opty.stage_name = '8-Closed Lost'
        THEN 1 ELSE 0
    END                                                         AS is_lost,

    CASE
    WHEN edm_opty.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN 0
    ELSE 1
    END                                                         AS is_open,

    CASE
    WHEN edm_opty.stage_name IN ('10-Duplicate')
        THEN 1
    ELSE 0
    END                                                         AS is_duplicate_flag,

    CASE
    WHEN edm_opty.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN 1
    ELSE 0
    END                                                         AS is_closed,

    CASE
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        THEN 1
    ELSE 0
    END                                                          AS is_renewal,

    -- date fields helpers
    close_date_detail.fiscal_quarter_name_fy                             AS close_fiscal_quarter_name,
    close_date_detail.first_day_of_fiscal_quarter                        AS close_fiscal_quarter_date,
    close_date_detail.fiscal_year                                        AS close_fiscal_year,
    close_date_detail.first_day_of_month                                 AS close_date_month,

    created_date_detail.fiscal_quarter_name_fy                           AS created_fiscal_quarter_name,
    created_date_detail.first_day_of_fiscal_quarter                      AS created_fiscal_quarter_date,
    created_date_detail.fiscal_year                                      AS created_fiscal_year,
    created_date_detail.first_day_of_month                               AS created_date_month,

    start_date.fiscal_quarter_name_fy                                    AS quote_start_date_fiscal_quarter_name,
    start_date.first_day_of_fiscal_quarter                               AS quote_start_date_fiscal_quarter_date,
    start_date.fiscal_year                                               AS quote_start_date_fiscal_year,
    start_date.first_day_of_month                                        AS quote_start_date_month,

    sales_accepted_date.fiscal_quarter_name_fy                           AS sales_accepted_fiscal_quarter_name,
    sales_accepted_date.first_day_of_fiscal_quarter                      AS sales_accepted_fiscal_quarter_date,
    sales_accepted_date.fiscal_year                                      AS sales_accepted_fiscal_year,
    sales_accepted_date.first_day_of_month                               AS sales_accepted_date_month,

    sales_qualified_date.fiscal_quarter_name_fy                          AS sales_qualified_fiscal_quarter_name,
    sales_qualified_date.first_day_of_fiscal_quarter                     AS sales_qualified_fiscal_quarter_date,
    sales_qualified_date.fiscal_year                                     AS sales_qualified_fiscal_year,
    sales_qualified_date.first_day_of_month                              AS sales_qualified_date_month,

    iacv_created_date.date_actual                                        AS net_arr_created_date,
    iacv_created_date.fiscal_quarter_name_fy                             AS net_arr_created_fiscal_quarter_name,
    iacv_created_date.first_day_of_fiscal_quarter                        AS net_arr_created_fiscal_quarter_date,
    iacv_created_date.fiscal_year                                        AS net_arr_created_fiscal_year,
    iacv_created_date.first_day_of_month                                 AS net_arr_created_date_month,

    iacv_created_date.date_actual                                        AS pipeline_created_date,
    iacv_created_date.fiscal_quarter_name_fy                             AS pipeline_created_fiscal_quarter_name,
    iacv_created_date.first_day_of_fiscal_quarter                        AS pipeline_created_fiscal_quarter_date,
    iacv_created_date.fiscal_year                                        AS pipeline_created_fiscal_year,
    iacv_created_date.first_day_of_month                                 AS pipeline_created_date_month,

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

    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    -- Opportunity User fields
    -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields

    -- Team Segment / ASM - RD
    -- NF 2022-01-28 Data seems clean in SFDC, but leaving the fallback just in case
    -- NF 2022-04-27 There are issues with the stamped field not reflecting the real owner of the opportunity
    --                adding is_open check here to default open deals to opportunity owners fields (instead of stamped)
    CASE
    WHEN sfdc_opportunity_xf.user_segment_stamped IS NULL
        OR edm_opty.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN opportunity_owner.user_segment
    ELSE sfdc_opportunity_xf.user_segment_stamped
    END                                                                   AS opportunity_owner_user_segment,

    CASE
    WHEN sfdc_opportunity_xf.user_geo_stamped IS NULL
        OR edm_opty.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN opportunity_owner.user_geo
    ELSE sfdc_opportunity_xf.user_geo_stamped
    END                                                                   AS opportunity_owner_user_geo,

    CASE
    WHEN sfdc_opportunity_xf.user_region_stamped IS NULL
            OR edm_opty.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN opportunity_owner.user_region
        ELSE sfdc_opportunity_xf.user_region_stamped
    END                                                                   AS opportunity_owner_user_region,

    CASE
    WHEN sfdc_opportunity_xf.user_area_stamped IS NULL
            OR  edm_opty.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        THEN opportunity_owner.user_area
    ELSE sfdc_opportunity_xf.user_area_stamped
    END                                                                   AS opportunity_owner_user_area,
    -- opportunity_owner_subarea_stamped

    ---------------------------
    ---------------------------

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

    -- JK 2022-06-16 temporary field for FO dashboard
    CASE sfdc_opportunity_raw.comp_new_logo_override__c
    WHEN 'Yes'
        THEN 1
    ELSE 0
    END                                 AS is_comp_new_logo_override,

    CASE
    WHEN edm_opty.stage_name
        IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN 1
    ELSE 0
    END                                                                   AS is_stage_1_plus,

    CASE
    WHEN edm_opty.stage_name
        IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN 1
    ELSE 0
END                                                                     AS is_stage_3_plus,


    CASE
    WHEN edm_opty.stage_name
        IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN 1
    ELSE 0
    END                                                                   AS is_stage_4_plus,


CASE
    WHEN edm_opty.stage_name
        IN ('0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')
        THEN 'Pipeline'
    WHEN edm_opty.stage_name
        IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN '3+ Pipeline'
    WHEN edm_opty.stage_name
        IN ('8-Closed Lost', 'Closed Lost')
        THEN 'Lost'
    WHEN edm_opty.stage_name IN ('Closed Won')
        THEN 'Closed Won'
    ELSE 'Other'
    END                                                                     AS stage_name_3plus,

CASE
    WHEN edm_opty.stage_name
        IN ('0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')
        THEN 'Pipeline'
    WHEN edm_opty.stage_name
        IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')
        THEN '4+ Pipeline'
    WHEN edm_opty.stage_name IN ('8-Closed Lost', 'Closed Lost')
        THEN 'Lost'
    WHEN edm_opty.stage_name IN ('Closed Won')
        THEN 'Closed Won'
    ELSE 'Other'
    END                                                                     AS stage_name_4plus,

    -- medium level grouping of the order type field
    CASE
    WHEN edm_opty.order_type = '1. New - First Order'
        THEN '1. New'
    WHEN edm_opty.order_type IN ('2. New - Connected', '3. Growth')
        THEN '2. Growth'
    WHEN edm_opty.order_type IN ('4. Contraction')
        THEN '3. Contraction'
    WHEN edm_opty.order_type IN ('5. Churn - Partial','6. Churn - Final')
        THEN '4. Churn'
    ELSE '5. Other'
    END                                                                   AS deal_category,

    CASE
    WHEN edm_opty.order_type = '1. New - First Order'
        THEN '1. New'
    WHEN edm_opty.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
        THEN '2. Growth'
    ELSE '3. Other'
    END                                                                   AS deal_group,

    -- fields for counting new logos, these fields count refund as negative
    CASE
    WHEN edm_opty.opportunity_category IN ('Decommission')
        THEN -1
    WHEN edm_opty.opportunity_category IN ('Credit')
        THEN 0
    ELSE 1
    END                                                                    AS calculated_deal_count,

    ----------------------------------------------------------------
    -- NF 2022-01-28 This is probably TO BE DEPRECATED too, need to align with Channel ops
    -- PIO Flag for PIO reporting dashboard
    CASE
    WHEN sfdc_opportunity_xf.dr_partner_engagement = 'PIO'
        THEN 1
    ELSE 0
    END                                                                    AS partner_engaged_opportunity_flag,

    -- check if renewal was closed on time or not
    CASE
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   >= sfdc_opportunity_xf.close_fiscal_quarter_date
        THEN 'On-Time'
    WHEN LOWER(edm_opty.sales_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   < sfdc_opportunity_xf.close_fiscal_quarter_date
        THEN 'Late'
    END                                                                       AS renewal_timing_status,

    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- calculated fields for pipeline velocity report

    -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
    CASE
    WHEN account.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc')
        AND sfdc_opportunity_xf.close_date < '2020-08-01'
        THEN 1
    -- NF 2021 - Pubsec extreme deals
    WHEN sfdc_opportunity_xf.opportunity_id IN ('0064M00000WtZKUQA3','0064M00000Xb975QAB')
        THEN 1
    -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
    WHEN sfdc_opportunity_xf.opportunity_id IN ('0064M00000ZGpfQQAT','0064M00000ZGpfVQAT','0064M00000ZGpfGQAT')
        THEN 1
    ELSE 0
    END                                                                       AS is_excluded_flag,
    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- NF 20220727 These next fields are needed for custom logic down the line
    sfdc_opportunity_xf.incremental_acv,
    sfdc_opportunity_xf.net_incremental_acv,
    sfdc_opportunity_xf.is_deleted
    -----------------------------------------------

FROM {{ref('sfdc_opportunity_xf')}} AS sfdc_opportunity_xf
-- FROM prod.restricted_safe_legacy.sfdc_opportunity_xf sfdc_opportunity_xf
INNER JOIN {{ref('mart_crm_opportunity')}} AS edm_opty
-- INNER JOIN prod.restricted_safe_common_mart_sales.mart_crm_opportunity edm_opty
    ON edm_opty.dim_crm_opportunity_id = sfdc_opportunity_xf.opportunity_id
-- not all fields are in opportunity xf
INNER JOIN {{ref('sfdc_opportunity')}} AS sfdc_opportunity
-- INNER JOIN prod.restricted_safe_legacy.sfdc_opportunity AS sfdc_opportunity
    ON sfdc_opportunity.opportunity_id = sfdc_opportunity_xf.opportunity_id
INNER JOIN {{ref('wk_sales_sfdc_users_xf')}} AS opportunity_owner
-- INNER JOIN prod.workspace_sales.sfdc_users_xf AS opportunity_owner
    ON opportunity_owner.user_id = sfdc_opportunity_xf.owner_id
-------------------------------------------
-------------------------------------------
-- Date helpers
INNER JOIN {{ref('wk_sales_date_details')}} AS close_date_detail
-- INNER JOIN prod.workspace_sales.date_details close_date_detail
    ON close_date_detail.date_actual = sfdc_opportunity_xf.close_date::DATE
INNER JOIN {{ref('wk_sales_date_details')}} AS created_date_detail
--INNER JOIN prod.workspace_sales.date_details created_date_detail
    ON created_date_detail.date_actual = sfdc_opportunity_xf.created_date::DATE
LEFT JOIN {{ref('wk_sales_date_details')}} AS sales_accepted_date
-- LEFT JOIN prod.workspace_sales.date_details sales_accepted_date
    ON sfdc_opportunity_xf.sales_accepted_date::DATE = sales_accepted_date.date_actual
LEFT JOIN {{ref('wk_sales_date_details')}} AS start_date
-- LEFT JOIN prod.workspace_sales.date_details start_date
    ON sfdc_opportunity_xf.subscription_start_date::DATE = start_date.date_actual

LEFT JOIN {{ref('wk_sales_date_details')}} AS sales_qualified_date
-- LEFT JOIN prod.workspace_sales.date_details sales_qualified_date
    ON sfdc_opportunity_xf.sales_qualified_date::DATE = sales_qualified_date.date_actual
LEFT JOIN {{ref('wk_sales_date_details')}} AS iacv_created_date
-- LEFT JOIN prod.workspace_sales.date_details iacv_created_date
    ON iacv_created_date.date_actual = sfdc_opportunity_xf.iacv_created_date::DATE
-- pipeline creation date
LEFT JOIN {{ref('wk_sales_date_details')}} AS stage_1_date
-- LEFT JOIN prod.workspace_sales.date_details stage_1_date
    ON stage_1_date.date_actual = sfdc_opportunity_xf.stage_1_discovery_date::date
-- pipeline creation date
LEFT JOIN {{ref('wk_sales_date_details')}} AS stage_3_date
-- LEFT JOIN prod.workspace_sales.date_details stage_3_date
    ON stage_3_date.date_actual = sfdc_opportunity_xf.stage_3_technical_evaluation_date::date
-- partner account details
LEFT JOIN {{ref('sfdc_accounts_xf')}} AS partner_account
-- LEFT JOIN prod.restricted_safe_legacy.sfdc_accounts_xf partner_account
    ON partner_account.account_id = sfdc_opportunity_xf.partner_account
-- NF 20211105 resale partner
LEFT JOIN {{ref('sfdc_accounts_xf')}} AS resale_account
-- LEFT JOIN prod.restricted_safe_legacy.sfdc_accounts_xf resale_account
    ON resale_account.account_id = sfdc_opportunity_xf.fulfillment_partner
-- JK 20220616 temp solution to add New Logo Override field
LEFT JOIN {{ source('salesforce', 'opportunity') }} AS sfdc_opportunity_raw
-- LEFT JOIN raw.salesforce_stitch.opportunity sfdc_opportunity_raw
    ON sfdc_opportunity.opportunity_id = sfdc_opportunity_raw.id
LEFT JOIN {{ref('sfdc_accounts_xf')}} AS account
-- LEFT JOIN prod.restricted_safe_legacy.sfdc_accounts_xf account
    ON account.account_id = sfdc_opportunity_xf.account_id
-- NF 20210906 remove JiHu opties from the models
WHERE sfdc_opportunity_xf.is_jihu_account = 0
