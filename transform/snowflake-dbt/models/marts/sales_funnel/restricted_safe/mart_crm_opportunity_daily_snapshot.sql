{{ simple_cte([
    ('fct_crm_opportunity','fct_crm_opportunity_daily_snapshot'),
    ('dim_crm_account','dim_crm_account_daily_snapshot'),
    ('dim_crm_user', 'dim_crm_user_daily_snapshot'),
    ('dim_date', 'dim_date')
]) }},

final AS (


  SELECT
    fct_crm_opportunity.snapshot_id,
    fct_crm_opportunity.crm_opportunity_snapshot_id,
    fct_crm_opportunity.dim_crm_opportunity_id,
    fct_crm_opportunity.opportunity_name,
    dim_crm_account.parent_crm_account_name,
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.dim_crm_account_id,
    fct_crm_opportunity.dim_crm_user_id,
    fct_crm_opportunity.duplicate_opportunity_id,

    -- dates
    fct_crm_opportunity.sales_accepted_date,
    fct_crm_opportunity.sales_accepted_month,
    fct_crm_opportunity.close_date,
    fct_crm_opportunity.close_month,
    fct_crm_opportunity.created_date,
    fct_crm_opportunity.snapshot_date,
    fct_crm_opportunity.snapshot_month,
    fct_crm_opportunity.snapshot_fiscal_year,
    fct_crm_opportunity.snapshot_fiscal_quarter_name,
    fct_crm_opportunity.snapshot_fiscal_quarter_date,
    fct_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    fct_crm_opportunity.snapshot_day_of_fiscal_year_normalised,
    fct_crm_opportunity.close_fiscal_year,
    fct_crm_opportunity.close_fiscal_quarter_name,
    fct_crm_opportunity.close_fiscal_quarter_date,
    fct_crm_opportunity.close_day_of_fiscal_quarter_normalised,
    fct_crm_opportunity.created_month,
    fct_crm_opportunity.created_fiscal_year,
    fct_crm_opportunity.created_fiscal_quarter_name,
    fct_crm_opportunity.created_fiscal_quarter_date,
    fct_crm_opportunity.iacv_created_date,
    fct_crm_opportunity.iacv_created_month,
    fct_crm_opportunity.iacv_created_fiscal_year,
    fct_crm_opportunity.iacv_created_fiscal_quarter_name,
    fct_crm_opportunity.iacv_created_fiscal_quarter_date,
    fct_crm_opportunity.net_arr_created_date,
    fct_crm_opportunity.net_arr_created_month,
    fct_crm_opportunity.net_arr_created_fiscal_year,
    fct_crm_opportunity.net_arr_created_fiscal_quarter_name,
    fct_crm_opportunity.net_arr_created_fiscal_quarter_date,
    fct_crm_opportunity.pipeline_created_date,
    fct_crm_opportunity.pipeline_created_month,
    fct_crm_opportunity.pipeline_created_fiscal_year,
    fct_crm_opportunity.pipeline_created_fiscal_quarter_name,
    fct_crm_opportunity.pipeline_created_fiscal_quarter_date,
    fct_crm_opportunity.sales_accepted_fiscal_year,
    fct_crm_opportunity.sales_accepted_fiscal_quarter_name,
    fct_crm_opportunity.sales_accepted_fiscal_quarter_date,
    fct_crm_opportunity.stage_0_pending_acceptance_date,
    fct_crm_opportunity.stage_1_discovery_date,
    fct_crm_opportunity.stage_2_scoping_date,
    fct_crm_opportunity.stage_3_technical_evaluation_date,
    fct_crm_opportunity.stage_4_proposal_date,
    fct_crm_opportunity.stage_5_negotiating_date,
    fct_crm_opportunity.stage_6_awaiting_signature_date,
    fct_crm_opportunity.stage_6_closed_won_date,
    fct_crm_opportunity.stage_6_closed_lost_date,
    fct_crm_opportunity.last_activity_date,
    fct_crm_opportunity.subscription_start_date,
    fct_crm_opportunity.subscription_end_date,

    -- opportunity attributes
    fct_crm_opportunity.stage_name,
    fct_crm_opportunity.reason_for_loss,
    fct_crm_opportunity.sales_type,
    fct_crm_opportunity.deal_path AS deal_path_name,
    fct_crm_opportunity.order_type,
    fct_crm_opportunity.order_type_grouped,
    fct_crm_opportunity.dr_partner_engagement AS dr_partner_engagement_name,
    fct_crm_opportunity.alliance_type AS alliance_type_name,
    fct_crm_opportunity.alliance_type_short AS alliance_type_short_name,
    fct_crm_opportunity.channel_type AS channel_type_name,
    fct_crm_opportunity.sales_qualified_source AS sales_qualified_source_name,
    fct_crm_opportunity.sales_qualified_source_grouped,
    fct_crm_opportunity.sqs_bucket_engagement,
    fct_crm_opportunity.closed_buckets,
    fct_crm_opportunity.opportunity_category,
    fct_crm_opportunity.source_buckets,
    fct_crm_opportunity.opportunity_sales_development_representative,
    fct_crm_opportunity.opportunity_business_development_representative,
    fct_crm_opportunity.opportunity_development_representative,
    fct_crm_opportunity.sdr_or_bdr,
    fct_crm_opportunity.iqm_submitted_by_role,
    fct_crm_opportunity.sdr_pipeline_contribution,
    fct_crm_opportunity.fpa_master_bookings_flag,
    fct_crm_opportunity.sales_path,
    fct_crm_opportunity.professional_services_value,
    fct_crm_opportunity.primary_solution_architect,
    fct_crm_opportunity.product_details,
    fct_crm_opportunity.product_category,
    fct_crm_opportunity.products_purchased,
    fct_crm_opportunity.growth_type,
    fct_crm_opportunity.opportunity_deal_size,
    fct_crm_opportunity.deployment_preference,
    fct_crm_opportunity.net_new_source_categories,
    fct_crm_opportunity.invoice_number,
    fct_crm_opportunity.primary_campaign_source_id,
    fct_crm_opportunity.ga_client_id,
    fct_crm_opportunity.opportunity_term,
    fct_crm_opportunity.cp_use_cases,
    fct_crm_opportunity.record_type_id,
    fct_crm_opportunity.opportunity_owner_manager,
    fct_crm_opportunity.account_owner_team_stamped,
    fct_crm_opportunity.stage_name_3plus,
    fct_crm_opportunity.stage_name_4plus,
    fct_crm_opportunity.stage_category,
    fct_crm_opportunity.deal_category,
    fct_crm_opportunity.deal_group,
    fct_crm_opportunity.deal_size,
    fct_crm_opportunity.calculated_deal_size,
    fct_crm_opportunity.dr_partner_engagement,
    fct_crm_opportunity.deal_path_engagement,
    fct_crm_opportunity.forecast_category_name,

    -- flags
    fct_crm_opportunity.is_won,
    fct_crm_opportunity.is_closed,
    fct_crm_opportunity.is_edu_oss,
    fct_crm_opportunity.is_ps_opp,
    fct_crm_opportunity.is_sao,
    fct_crm_opportunity.is_net_arr_closed_deal,
    fct_crm_opportunity.is_new_logo_first_order,
    fct_crm_opportunity.is_net_arr_pipeline_created,
    fct_crm_opportunity.is_win_rate_calc,
    fct_crm_opportunity.is_closed_won,
    fct_crm_opportunity.is_web_portal_purchase,
    fct_crm_opportunity.is_stage_1_plus,
    fct_crm_opportunity.is_stage_3_plus,
    fct_crm_opportunity.is_stage_4_plus,
    fct_crm_opportunity.is_lost,
    fct_crm_opportunity.is_open,
    fct_crm_opportunity.is_renewal,
    fct_crm_opportunity.is_refund,
    fct_crm_opportunity.is_deleted,

    -- account fields
    dim_crm_account.is_jihu_account,
    dim_crm_account.fy22_new_logo_target_list,
    dim_crm_account.crm_account_gtm_strategy,
    dim_crm_account.crm_account_focus_account,
    dim_crm_account.crm_account_zi_technologies,
    dim_crm_account.parent_crm_account_gtm_strategy,
    dim_crm_account.parent_crm_account_focus_account,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.parent_crm_account_zi_technologies,
    dim_crm_account.parent_crm_account_demographics_sales_segment,
    dim_crm_account.parent_crm_account_demographics_geo,
    dim_crm_account.parent_crm_account_demographics_region,
    dim_crm_account.parent_crm_account_demographics_area,
    dim_crm_account.parent_crm_account_demographics_territory,
    dim_crm_account.crm_account_demographics_employee_count,
    dim_crm_account.parent_crm_account_demographics_max_family_employee,
    dim_crm_account.parent_crm_account_demographics_upa_country,
    dim_crm_account.parent_crm_account_demographics_upa_state,
    dim_crm_account.parent_crm_account_demographics_upa_city,
    dim_crm_account.parent_crm_account_demographics_upa_street,
    dim_crm_account.parent_crm_account_demographics_upa_postal_code,

    -- crm opp owner/account owner fields stamped at SAO date
    fct_crm_opportunity.sao_crm_opp_owner_stamped_name,
    fct_crm_opportunity.sao_crm_account_owner_stamped_name,
    fct_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
    fct_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
    fct_crm_opportunity.sao_crm_opp_owner_geo_stamped,
    fct_crm_opportunity.sao_crm_opp_owner_region_stamped,
    fct_crm_opportunity.sao_crm_opp_owner_area_stamped,
    fct_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
    fct_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,

    -- crm opp owner/account owner stamped fields stamped at close date
    fct_crm_opportunity.crm_opp_owner_stamped_name,
    fct_crm_opportunity.crm_account_owner_stamped_name,
    fct_crm_opportunity.user_segment_stamped AS crm_opp_owner_sales_segment_stamped,
    fct_crm_opportunity.user_segment_stamped_grouped AS crm_opp_owner_sales_segment_stamped_grouped,
    fct_crm_opportunity.user_geo_stamped AS crm_opp_owner_geo_stamped,
    fct_crm_opportunity.user_region_stamped AS crm_opp_owner_region_stamped,
    fct_crm_opportunity.user_area_stamped AS crm_opp_owner_area_stamped,
    {{ sales_segment_region_grouped('fct_crm_opportunity.user_segment_stamped',
        'fct_crm_opportunity.user_geo_stamped', 'fct_crm_opportunity.user_region_stamped') }}
    AS crm_opp_owner_sales_segment_region_stamped_grouped,
    fct_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,
    fct_crm_opportunity.crm_opp_owner_user_role_type_stamped,

    -- crm owner/sales rep live fields
    opp_owner_live.crm_user_sales_segment,
    opp_owner_live.crm_user_sales_segment_grouped,
    opp_owner_live.crm_user_geo,
    opp_owner_live.crm_user_region,
    opp_owner_live.crm_user_area,
    {{ sales_segment_region_grouped('opp_owner_live.crm_user_sales_segment',
        'opp_owner_live.crm_user_geo', 'opp_owner_live.crm_user_region') }}
    AS crm_user_sales_segment_region_grouped,

    -- crm account owner/sales rep live fields
    account_owner_live.crm_user_sales_segment AS crm_account_user_sales_segment,
    account_owner_live.crm_user_sales_segment_grouped AS crm_account_user_sales_segment_grouped,
    account_owner_live.crm_user_geo AS crm_account_user_geo,
    account_owner_live.crm_user_region AS crm_account_user_region,
    account_owner_live.crm_user_area AS crm_account_user_area,
    {{ sales_segment_region_grouped('account_owner_live.crm_user_sales_segment',
        'account_owner_live.crm_user_geo', 'account_owner_live.crm_user_region') }}
    AS crm_account_user_sales_segment_region_grouped,

    -- Pipeline Velocity Account and Opp Owner Fields and Key Reporting Fields
    fct_crm_opportunity.opportunity_owner_user_segment,
    fct_crm_opportunity.opportunity_owner_user_geo,
    fct_crm_opportunity.opportunity_owner_user_region,
    fct_crm_opportunity.opportunity_owner_user_area,
    LOWER(
      fct_crm_opportunity.user_segment_stamped
    ) AS report_opportunity_user_segment,
    LOWER(
      fct_crm_opportunity.user_geo_stamped
    ) AS report_opportunity_user_geo,
    LOWER(
      fct_crm_opportunity.user_region_stamped
    ) AS report_opportunity_user_region,
    LOWER(
      fct_crm_opportunity.user_area_stamped
    ) AS report_opportunity_user_area,
    LOWER(
      fct_crm_opportunity.order_type
    ) AS order_type_stamped,
    LOWER(
      account_owner_live.crm_user_sales_segment
    ) AS account_owner_user_segment,
    LOWER(
      account_owner_live.crm_user_geo
    ) AS account_owner_user_geo,
    LOWER(
      account_owner_live.crm_user_region
    ) AS account_owner_user_region,
    LOWER(
      account_owner_live.crm_user_area
    ) AS account_owner_user_area,
    LOWER(
      fct_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped
    ) AS report_user_segment_geo_region_area,
    LOWER(
      CONCAT(
        report_user_segment_geo_region_area,
        '-',
        fct_crm_opportunity.sales_qualified_source,
        '-',
        fct_crm_opportunity.order_type
      )
    ) AS report_user_segment_geo_region_area_sqs_ot,
    report_opportunity_user_segment AS key_segment,
    fct_crm_opportunity.sales_qualified_source AS key_sqs,
    fct_crm_opportunity.deal_group AS key_ot,
    report_opportunity_user_segment || '_' || fct_crm_opportunity.sales_qualified_source AS key_segment_sqs,
    report_opportunity_user_segment || '_' || fct_crm_opportunity.deal_group AS key_segment_ot,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo AS key_segment_geo,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || fct_crm_opportunity.sales_qualified_source AS key_segment_geo_sqs,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || fct_crm_opportunity.deal_group AS key_segment_geo_ot,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region AS key_segment_geo_region,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || fct_crm_opportunity.sales_qualified_source AS key_segment_geo_region_sqs,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || fct_crm_opportunity.deal_group AS key_segment_geo_region_ot,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area AS key_segment_geo_region_area,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || fct_crm_opportunity.sales_qualified_source AS key_segment_geo_region_area_sqs,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || fct_crm_opportunity.deal_group AS key_segment_geo_region_area_ot,
    report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_area AS key_segment_geo_area,
    COALESCE(
      report_opportunity_user_segment, 'other'
    ) AS sales_team_cro_level,
    -- This code replicates the reporting structured of FY22, to keep current tools working
    CASE
      WHEN report_opportunity_user_segment = 'large'
        AND report_opportunity_user_geo = 'emea'
        THEN 'large_emea'
      WHEN report_opportunity_user_segment = 'mid-market'
        AND report_opportunity_user_region = 'amer'
        AND LOWER(report_opportunity_user_area) LIKE '%west%'
        THEN 'mid-market_west'
      WHEN report_opportunity_user_segment = 'mid-market'
        AND report_opportunity_user_region = 'amer'
        AND LOWER(report_opportunity_user_area) NOT LIKE '%west%'
        THEN 'mid-market_east'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'amer'
        AND LOWER(report_opportunity_user_area) LIKE '%west%'
        THEN 'smb_west'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'amer'
        AND LOWER(report_opportunity_user_area) NOT LIKE '%west%'
        THEN 'smb_east'
      WHEN report_opportunity_user_segment = 'smb'
        AND report_opportunity_user_region = 'latam'
        THEN 'smb_east'
      WHEN (report_opportunity_user_segment IS NULL
        OR report_opportunity_user_region IS NULL)
        THEN 'other'
      WHEN
        CONCAT(report_opportunity_user_segment, '_', report_opportunity_user_region) LIKE '%other%'
        THEN 'other'
      ELSE CONCAT(report_opportunity_user_segment, '_', report_opportunity_user_region)
    END AS sales_team_rd_asm_level,
    COALESCE(
      CONCAT(report_opportunity_user_segment, '_', report_opportunity_user_geo), 'other'
    ) AS sales_team_vp_level,
    COALESCE(
      CONCAT(
        report_opportunity_user_segment,
        '_',
        report_opportunity_user_geo,
        '_',
        report_opportunity_user_region
      ),
      'other'
    ) AS sales_team_avp_rd_level,
    COALESCE(
      CONCAT(
        report_opportunity_user_segment,
        '_',
        report_opportunity_user_geo,
        '_',
        report_opportunity_user_region,
        '_',
        report_opportunity_user_area
      ),
      'other'
    ) AS sales_team_asm_level,
    CASE
      WHEN
        fct_crm_opportunity.account_owner_team_stamped IN (
          'Commercial - SMB', 'SMB', 'SMB - US', 'SMB - International'
        )
        THEN 'SMB'
      WHEN
        fct_crm_opportunity.account_owner_team_stamped IN (
          'APAC', 'EMEA', 'Channel', 'US West', 'US East', 'Public Sector'
        )
        THEN 'Large'
      WHEN
        fct_crm_opportunity.account_owner_team_stamped IN (
          'MM - APAC', 'MM - East', 'MM - EMEA', 'Commercial - MM', 'MM - West', 'MM-EMEA'
        )
        THEN 'Mid-Market'
      ELSE 'SMB'
    END AS account_owner_team_stamped_cro_level,

    -- channel fields
    fct_crm_opportunity.lead_source,
    fct_crm_opportunity.dr_partner_deal_type,
    fct_crm_opportunity.partner_account,
    fct_crm_opportunity.dr_status,
    fct_crm_opportunity.distributor,
    fct_crm_opportunity.dr_deal_id,
    fct_crm_opportunity.dr_primary_registration,
    fct_crm_opportunity.influence_partner,
    fct_crm_opportunity.fulfillment_partner,
    fct_crm_opportunity.platform_partner,
    fct_crm_opportunity.partner_track,
    fct_crm_opportunity.resale_partner_track,
    fct_crm_opportunity.is_public_sector_opp,
    fct_crm_opportunity.is_registration_from_portal,
    fct_crm_opportunity.calculated_discount,
    fct_crm_opportunity.partner_discount,
    fct_crm_opportunity.partner_discount_calc,
    fct_crm_opportunity.comp_channel_neutral,
    fct_crm_opportunity.count_crm_attribution_touchpoints,
    fct_crm_opportunity.weighted_linear_iacv,
    fct_crm_opportunity.count_campaigns,

    -- Solutions-Architech fields
    fct_crm_opportunity.sa_tech_evaluation_close_status,
    fct_crm_opportunity.sa_tech_evaluation_end_date,
    fct_crm_opportunity.sa_tech_evaluation_start_date,

    -- Command Plan fields
    fct_crm_opportunity.cp_partner,
    fct_crm_opportunity.cp_paper_process,
    fct_crm_opportunity.cp_help,
    fct_crm_opportunity.cp_review_notes,

    -- Competitor flags
    fct_crm_opportunity.competitors,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Other')
        THEN 1
      ELSE 0
    END AS competitors_other_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'GitLab Core')
        THEN 1
      ELSE 0
    END AS competitors_gitlab_core_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'None')
        THEN 1
      ELSE 0
    END AS competitors_none_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'GitHub Enterprise')
        THEN 1
      ELSE 0
    END AS competitors_github_enterprise_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'BitBucket Server')
        THEN 1
      ELSE 0
    END AS competitors_bitbucket_server_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Unknown')
        THEN 1
      ELSE 0
    END AS competitors_unknown_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'GitHub.com')
        THEN 1
      ELSE 0
    END AS competitors_github_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'GitLab.com')
        THEN 1
      ELSE 0
    END AS competitors_gitlab_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Jenkins')
        THEN 1
      ELSE 0
    END AS competitors_jenkins_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Azure DevOps')
        THEN 1
      ELSE 0
    END AS competitors_azure_devops_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'SVN')
        THEN 1
      ELSE 0
    END AS competitors_svn_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'BitBucket.Org')
        THEN 1
      ELSE 0
    END AS competitors_bitbucket_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Atlassian')
        THEN 1
      ELSE 0
    END AS competitors_atlassian_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Perforce')
        THEN 1
      ELSE 0
    END AS competitors_perforce_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Visual Studio Team Services')
        THEN 1
      ELSE 0
    END AS competitors_visual_studio_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Azure')
        THEN 1
      ELSE 0
    END AS competitors_azure_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Amazon Code Commit')
        THEN 1
      ELSE 0
    END AS competitors_amazon_code_commit_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'CircleCI')
        THEN 1
      ELSE 0
    END AS competitors_circleci_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'Bamboo')
        THEN 1
      ELSE 0
    END AS competitors_bamboo_flag,
    CASE
      WHEN CONTAINS(fct_crm_opportunity.competitors, 'AWS')
        THEN 1
      ELSE 0
    END AS competitors_aws_flag,

    -- additive fields
    fct_crm_opportunity.days_in_sao,
    fct_crm_opportunity.arr_basis,
    fct_crm_opportunity.incremental_acv AS iacv,
    fct_crm_opportunity.net_incremental_acv AS net_iacv,
    fct_crm_opportunity.net_arr,
    fct_crm_opportunity.new_logo_count,
    fct_crm_opportunity.amount,
    fct_crm_opportunity.open_1plus_deal_count,
    fct_crm_opportunity.open_3plus_deal_count,
    fct_crm_opportunity.open_4plus_deal_count,
    fct_crm_opportunity.booked_deal_count,
    fct_crm_opportunity.churned_contraction_deal_count,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.open_4plus_net_arr,
    fct_crm_opportunity.booked_net_arr,
    fct_crm_opportunity.churned_contraction_net_arr,
    fct_crm_opportunity.opportunity_based_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_from_ratio_net_arr,
    fct_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_deal_count,
    fct_crm_opportunity.acv,
    fct_crm_opportunity.incremental_acv,
    fct_crm_opportunity.net_incremental_acv,
    fct_crm_opportunity.raw_net_arr,
    fct_crm_opportunity.arr,
    fct_crm_opportunity.recurring_amount,
    fct_crm_opportunity.true_up_amount,
    fct_crm_opportunity.proserv_amount,
    fct_crm_opportunity.other_non_recurring_amount,
    fct_crm_opportunity.refund_iacv,
    fct_crm_opportunity.downgrade_iacv,
    fct_crm_opportunity.renewal_acv,
    fct_crm_opportunity.renewal_amount,
    fct_crm_opportunity.total_contract_value,

    -- calculated fields 
    fct_crm_opportunity.calculated_age_in_days,
    CASE
      WHEN fct_crm_opportunity.pipeline_created_fiscal_quarter_name = fct_crm_opportunity.snapshot_fiscal_quarter_name
        AND fct_crm_opportunity.is_eligible_created_pipeline = 1
        THEN fct_crm_opportunity.net_arr
      ELSE 0
    END AS created_in_snapshot_quarter_net_arr,
    CASE
      WHEN fct_crm_opportunity.pipeline_created_fiscal_quarter_name = fct_crm_opportunity.close_fiscal_quarter_name
        AND fct_crm_opportunity.is_won = 1
        AND fct_crm_opportunity.is_eligible_created_pipeline = 1
        THEN fct_crm_opportunity.net_arr
      ELSE 0
    END AS created_and_won_same_quarter_net_arr,
    CASE
      WHEN fct_crm_opportunity.pipeline_created_fiscal_quarter_name = fct_crm_opportunity.snapshot_fiscal_quarter_name
        AND fct_crm_opportunity.is_eligible_created_pipeline = 1
        THEN fct_crm_opportunity.calculated_deal_count
      ELSE 0
    END AS created_in_snapshot_quarter_deal_count,

    -- renamed columns for pipeline & velocity reporting
    fct_crm_opportunity.dim_crm_account_id AS raw_account_id,
    fct_crm_opportunity.crm_opportunity_snapshot_id AS opportunity_snapshot_id,
    fct_crm_opportunity.dim_crm_opportunity_id AS opportunity_id,
    fct_crm_opportunity.dim_crm_user_id AS owner_id,
    fct_crm_opportunity.merged_opportunity_id,
    fct_crm_opportunity.opportunity_owner_department,
    fct_crm_opportunity.order_type AS snapshot_order_type_stamped,
    fct_crm_opportunity.is_credit AS is_credit_flag,
    fct_crm_opportunity.is_contract_reset AS is_contract_reset_flag,
    fct_crm_opportunity.sales_qualified_source AS snapshot_sales_qualified_source,
    fct_crm_opportunity.is_edu_oss AS snapshot_is_edu_oss,
    fct_crm_opportunity.dim_crm_account_id AS account_id,
    fct_crm_opportunity.stage_1_discovery_date AS stage_1_date,
    fct_crm_opportunity.stage_1_discovery_month AS stage_1_date_month,
    fct_crm_opportunity.stage_1_discovery_fiscal_year AS stage_1_fiscal_year,
    fct_crm_opportunity.stage_1_discovery_fiscal_quarter_name AS stage_1_fiscal_quarter_name,
    fct_crm_opportunity.stage_1_discovery_fiscal_quarter_date AS stage_1_fiscal_quarter_date,
    fct_crm_opportunity.is_duplicate AS current_is_duplicate_flag,
    fct_crm_opportunity.opportunity_owner,
    dim_crm_account.crm_account_name AS account_name,
    dim_crm_account.crm_account_tsp_region AS tsp_region,
    dim_crm_account.crm_account_tsp_sub_region AS tsp_sub_region,
    dim_crm_account.parent_crm_account_sales_segment AS ultimate_parent_sales_segment,
    dim_crm_account.tsp_max_hierarchy_sales_segment,
    dim_crm_account.dim_parent_crm_account_id AS ultimate_parent_account_id,
    dim_crm_account.parent_crm_account_name AS ultimate_parent_account_name,
    dim_crm_account.dim_parent_crm_account_id AS ultimate_parent_id,
    dim_crm_account.parent_crm_account_demographics_sales_segment AS account_demographics_segment,
    dim_crm_account.parent_crm_account_demographics_geo AS account_demographics_geo,
    dim_crm_account.parent_crm_account_demographics_region AS account_demographics_region,
    dim_crm_account.parent_crm_account_demographics_area AS account_demographics_area,
    dim_crm_account.parent_crm_account_demographics_territory AS account_demographics_territory,
    dim_crm_account.parent_crm_account_demographics_sales_segment AS upa_demographics_segment,
    dim_crm_account.parent_crm_account_demographics_geo AS upa_demographics_geo,
    dim_crm_account.parent_crm_account_demographics_region AS upa_demographics_region,
    dim_crm_account.parent_crm_account_demographics_area AS upa_demographics_area,
    dim_crm_account.parent_crm_account_demographics_territory AS upa_demographics_territory,
    fct_crm_opportunity.is_eligible_open_pipeline AS is_eligible_open_pipeline_flag,
    fct_crm_opportunity.is_eligible_created_pipeline AS is_eligible_created_pipeline_flag,
    fct_crm_opportunity.is_eligible_sao AS is_eligible_sao_flag,
    fct_crm_opportunity.is_eligible_asp_analysis AS is_eligible_asp_analysis_flag,
    fct_crm_opportunity.is_eligible_age_analysis AS is_eligible_age_analysis_flag,
    fct_crm_opportunity.fpa_master_bookings_flag AS is_booked_net_arr_flag,
    fct_crm_opportunity.is_eligible_churn_contraction AS is_eligible_churn_contraction_flag,
    fct_crm_opportunity.snapshot_month AS snapshot_date_month,
    fct_crm_opportunity.close_month AS close_date_month,
    fct_crm_opportunity.created_month AS created_date_month,
    fct_crm_opportunity.iacv_created_month AS iacv_created_date_month,
    fct_crm_opportunity.net_arr_created_month AS net_arr_created_date_month,
    fct_crm_opportunity.pipeline_created_month AS pipeline_created_date_month,
    fct_crm_opportunity.is_excluded AS is_excluded_flag

  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_account
    ON fct_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
      AND fct_crm_opportunity.snapshot_id = dim_crm_account.snapshot_id
  LEFT JOIN dim_crm_user AS opp_owner_live
    ON fct_crm_opportunity.dim_crm_user_id = opp_owner_live.dim_crm_user_id
      AND fct_crm_opportunity.snapshot_id = opp_owner_live.snapshot_id
  LEFT JOIN dim_crm_user AS account_owner_live
    ON dim_crm_account.dim_crm_user_id = account_owner_live.dim_crm_user_id
      AND dim_crm_account.snapshot_id = account_owner_live.snapshot_id


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jpeguero",
    created_date="2022-05-05",
    updated_date="2022-08-03"
  ) }}
