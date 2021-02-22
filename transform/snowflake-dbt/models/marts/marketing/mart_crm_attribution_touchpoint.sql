{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('dim_crm_touchpoint','dim_crm_touchpoint'),
    ('fct_crm_attribution_touchpoint','fct_crm_attribution_touchpoint'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'),
    ('dim_crm_person','dim_crm_person'),
    ('fct_crm_person', 'fct_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_sales_representative','dim_crm_sales_representative'),
    ('mart_crm_opportunity','mart_crm_opportunity')
]) }}

, final AS (

    SELECT
      -- touchpoint info
      dim_crm_touchpoint.dim_crm_touchpoint_id,
      dim_crm_touchpoint.bizible_touchpoint_date,
      dim_crm_touchpoint.bizible_touchpoint_position,
      dim_crm_touchpoint.bizible_touchpoint_source,
      dim_crm_touchpoint.bizible_ad_campaign_name,
      dim_crm_touchpoint.bizible_ad_content,
      dim_crm_touchpoint.bizible_ad_group_name,
      dim_crm_touchpoint.bizible_form_url,
      dim_crm_touchpoint.bizible_form_url_raw,
      dim_crm_touchpoint.bizible_landing_page,
      dim_crm_touchpoint.bizible_landing_page_raw,
      dim_crm_touchpoint.bizible_marketing_channel,
      dim_crm_touchpoint.bizible_marketing_channel_path,
      dim_crm_touchpoint.bizible_medium,
      dim_crm_touchpoint.bizible_referrer_page,
      dim_crm_touchpoint.bizible_referrer_page_raw,
      fct_crm_attribution_touchpoint.bizible_count_first_touch,
      fct_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      fct_crm_attribution_touchpoint.bizible_attribution_percent_full_path,
      fct_crm_attribution_touchpoint.bizible_count_u_shaped,
      fct_crm_attribution_touchpoint.bizible_count_w_shaped,
      fct_crm_attribution_touchpoint.bizible_revenue_full_path,
      fct_crm_attribution_touchpoint.bizible_revenue_custom_model,
      fct_crm_attribution_touchpoint.bizible_revenue_first_touch,
      fct_crm_attribution_touchpoint.bizible_revenue_lead_conversion,
      fct_crm_attribution_touchpoint.bizible_revenue_u_shaped,
      fct_crm_attribution_touchpoint.bizible_revenue_w_shaped,

      -- person info
      fct_crm_attribution_touchpoint.dim_crm_person_id,
      dim_crm_person.sfdc_record_id,
      dim_crm_person.sfdc_record_type,
      dim_crm_person.email_hash,
      dim_crm_person.email_domain,
      dim_crm_person.owner_id,
      dim_crm_person.person_score,
      dim_crm_person.title                                                  AS crm_person_title,
      dim_crm_person.status                                                 AS crm_person_status,
      dim_crm_person.lead_source,
      dim_crm_person.lead_source_type,
      dim_crm_person.source_buckets                                         AS crm_person_source_buckets,
      dim_crm_person.net_new_source_categories,
      fct_crm_person.created_date                                           AS crm_person_created_date,
      fct_crm_person.inquiry_date,
      fct_crm_person.mql_date_first,
      fct_crm_person.mql_date_latest,
      fct_crm_person.accepted_date,
      fct_crm_person.qualifying_date,
      fct_crm_person.qualified_date,
      fct_crm_person.converted_date,
      fct_crm_person.is_mql,
      fct_crm_person.is_inquiry,
      fct_crm_person.mql_count,

      -- campaign info
      dim_campaign.campaign_name,
      dim_campaign.is_active                                                AS campaign_is_active,
      dim_campaign.status                                                   AS campagin_status,
      dim_campaign.type,
      dim_campaign.description,
      dim_campaign.budget_holder,
      dim_campaign.bizible_touchpoint_enabled_setting,
      dim_campaign.strategic_marketing_contribution,
      fct_campaign.campaign_parent_id,
      fct_campaign.campaign_owner_id,
      fct_campaign.created_by_id                                            AS campaign_created_by_id,
      fct_campaign.start_date                                               AS camapaign_start_date,
      fct_campaign.end_date                                                 AS campaign_end_date,
      fct_campaign.created_date                                             AS campaign_created_date,
      fct_campaign.last_modified_date                                       AS campaign_last_modified_date,
      fct_campaign.last_activity_date                                       AS campaign_last_activity_date,
      fct_campaign.region                                                   AS campaign_region,
      fct_campaign.sub_region                                               AS campaign_sub_region,
      fct_campaign.budgeted_cost,
      fct_campaign.expected_response,
      fct_campaign.expected_revenue,
      fct_campaign.actual_cost,
      fct_campaign.amount_all_opportunities,
      fct_campaign.amount_won_opportunities,
      fct_campaign.count_contacts,
      fct_campaign.count_converted_leads,
      fct_campaign.count_leads,
      fct_campaign.count_opportunities,
      fct_campaign.count_responses,
      fct_campaign.count_won_opportunities,
      fct_campaign.count_sent,

      -- sales rep info
      dim_crm_sales_representative.rep_name,
      dim_crm_sales_representative.title                                    AS rep_title,
      dim_crm_sales_representative.team,
      dim_crm_sales_representative.is_active                                AS rep_is_active,
      dim_crm_sales_representative.user_role_name,
      dim_crm_sales_representative.sales_segment_name_live                  AS touchpoint_crm_user_segment_name_live,
      dim_crm_sales_representative.location_region_name_live                AS touchpoint_crm_user_geo_name_live,
      dim_crm_sales_representative.sales_region_name_live                   AS touchpoint_crm_user_region_name_live,
      dim_crm_sales_representative.sales_area_name_live                     AS touchpoint_crm_user_area_name_live,

      -- account info
      dim_crm_account.crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.crm_account_industry,
      dim_crm_account.crm_account_account_owner_team,
      dim_crm_account.crm_account_territory,
      dim_crm_account.crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.health_score,
      dim_crm_account.health_number,
      dim_crm_account.health_score_color,
      dim_crm_account.ultimate_parent_account_id,
      dim_crm_account.ultimate_parent_account_name,
      dim_crm_account.ultimate_parent_account_segment,
      dim_crm_account.ultimate_parent_billing_country,
      dim_crm_account.ultimate_parent_industry,
      dim_crm_account.ultimate_parent_account_owner_team,
      dim_crm_account.ultimate_parent_territory,
      dim_crm_account.ultimate_parent_tsp_region,
      dim_crm_account.ultimate_parent_tsp_sub_region,
      dim_crm_account.ultimate_parent_tsp_area,
      dim_crm_account.ultimate_parent_gtm_strategy,
      dim_crm_account.ultimate_parent_focus_account,
      dim_crm_account.record_type_id,
      dim_crm_account.federal_account,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.account_owner,
      dim_crm_account.account_owner_team,
      dim_crm_account.account_type,
      dim_crm_account.gtm_strategy,
      dim_crm_account.technical_account_manager,
      dim_crm_account.is_deleted,
      dim_crm_account.merged_to_account_id,
      dim_crm_account.is_reseller,

      -- opportunity info
      fct_crm_attribution_touchpoint.dim_crm_opportunity_id,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.sales_accepted_month,
      mart_crm_opportunity.close_date                                       AS opportunity_close_date,
      mart_crm_opportunity.close_month                                      AS opportunity_close_month,
      mart_crm_opportunity.created_date                                     AS opportunity_created_date,
      mart_crm_opportunity.created_month                                    AS opportunity_created_month,
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.is_closed,
      mart_crm_opportunity.days_in_sao,
      mart_crm_opportunity.iacv,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.amount,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.reason_for_loss,
      mart_crm_opportunity.is_sao,
      mart_crm_opportunity.sales_segment_name_stamped                       AS crm_user_sales_segment_name_stamped,
      mart_crm_opportunity.location_region_name_stamped                     AS crm_user_geo_name_stamped,
      mart_crm_opportunity.sales_region_name_stamped                        AS crm_user_region_name_stamped,
      mart_crm_opportunity.sales_area_name_stamped                          AS crm_user_area_name_stamped,
      mart_crm_opportunity.sales_segment_name_live,
      mart_crm_opportunity.location_region_name_live,
      mart_crm_opportunity.sales_region_name_live,
      mart_crm_opportunity.sales_area_name_live,
      mart_crm_opportunity.purchase_channel_name,
      mart_crm_opportunity.order_type,
      mart_crm_opportunity.opportunity_source_name,
      mart_crm_opportunity.closed_buckets,
      mart_crm_opportunity.source_buckets                                   AS opportunity_source_buckets,
      mart_crm_opportunity.opportunity_sales_development_representative,
      mart_crm_opportunity.opportunity_business_development_representative,
      mart_crm_opportunity.opportunity_development_representative,
      mart_crm_opportunity.is_web_portal_purchase

    FROM fct_crm_attribution_touchpoint
    LEFT JOIN dim_crm_touchpoint
      ON fct_crm_attribution_touchpoint.dim_crm_touchpoint_id = dim_crm_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN dim_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = dim_campaign.campaign_id
    LEFT JOIN fct_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = fct_campaign.dim_campaign_id
    LEFT JOIN dim_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN fct_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = fct_crm_person.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON fct_crm_attribution_touchpoint.dim_crm_account_id = dim_crm_account.crm_account_id
    LEFT JOIN dim_crm_sales_representative
      ON fct_crm_attribution_touchpoint.dim_crm_sales_rep_id = dim_crm_sales_representative.dim_crm_sales_rep_id
    LEFT JOIN mart_crm_opportunity
      ON fct_crm_attribution_touchpoint.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-02-18",
    updated_date="2020-02-18"
) }}
