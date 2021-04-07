{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('dim_crm_touchpoint','dim_crm_touchpoint'),
    ('fct_crm_touchpoint','fct_crm_touchpoint'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'),
    ('dim_crm_person','dim_crm_person'),
    ('fct_crm_person', 'fct_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_sales_representative','dim_crm_sales_representative')
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
      dim_crm_touchpoint.bizible_integrated_campaign_grouping,
      dim_crm_touchpoint.touchpoint_segment,
      dim_crm_touchpoint.gtm_motion,
      dim_crm_touchpoint.integrated_campaign_grouping,
      fct_crm_touchpoint.bizible_count_first_touch,
      fct_crm_touchpoint.bizible_count_lead_creation_touch,
      fct_crm_touchpoint.bizible_count_u_shaped,

      -- person info
      fct_crm_touchpoint.dim_crm_person_id,
      dim_crm_person.sfdc_record_id,
      dim_crm_person.sfdc_record_type,
      dim_crm_person.email_hash,
      dim_crm_person.email_domain,
      dim_crm_person.owner_id,
      dim_crm_person.person_score,
      dim_crm_person.title                                                 AS crm_person_title,
      dim_crm_person.status                                                AS crm_person_status,
      dim_crm_person.lead_source,
      dim_crm_person.lead_source_type,
      dim_crm_person.source_buckets,
      dim_crm_person.net_new_source_categories,
      fct_crm_person.created_date                                          AS crm_person_created_date,
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
      dim_campaign.is_active                                               AS campagin_is_active,
      dim_campaign.status                                                  AS campaign_status,
      dim_campaign.type,
      dim_campaign.description,
      dim_campaign.budget_holder,
      dim_campaign.bizible_touchpoint_enabled_setting,
      dim_campaign.strategic_marketing_contribution,
      fct_campaign.dim_parent_campaign_id,
      fct_campaign.campaign_owner_id,
      fct_campaign.created_by_id                                           AS campaign_created_by_id,
      fct_campaign.start_date                                              AS camapaign_start_date,
      fct_campaign.end_date                                                AS campaign_end_date,
      fct_campaign.created_date                                            AS campaign_created_date,
      fct_campaign.last_modified_date                                      AS campaign_last_modified_date,
      fct_campaign.last_activity_date                                      AS campaign_last_activity_date,
      fct_campaign.region                                                  AS campaign_region,
      fct_campaign.sub_region                                              AS campaign_sub_region,
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
      dim_crm_sales_representative.title                                   AS rep_title,
      dim_crm_sales_representative.team,
      dim_crm_sales_representative.is_active                               AS rep_is_active,
      dim_crm_sales_representative.user_role_name,
      dim_crm_sales_representative.sales_segment_name_live                 AS campaign_crm_user_segment_name_live,
      dim_crm_sales_representative.location_region_name_live               AS campaign_crm_user_geo_name_live,
      dim_crm_sales_representative.sales_region_name_live                  AS campaign_crm_user_region_name_live,
      dim_crm_sales_representative.sales_area_name_live                    AS campaign_crm_user_area_name_live,

      -- account info
      dim_crm_account.dim_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.crm_account_industry,
      dim_crm_account.crm_account_owner_team,
      dim_crm_account.crm_account_sales_territory,
      dim_crm_account.crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.health_score,
      dim_crm_account.health_number,
      dim_crm_account.health_score_color,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory,
      dim_crm_account.parent_crm_account_tsp_region,
      dim_crm_account.parent_crm_account_tsp_sub_region,
      dim_crm_account.parent_crm_account_tsp_area,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.record_type_id,
      dim_crm_account.federal_account,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.crm_account_type,
      dim_crm_account.technical_account_manager,
      dim_crm_account.is_deleted,
      dim_crm_account.merged_to_account_id,
      dim_crm_account.is_reseller

    FROM fct_crm_touchpoint
    LEFT JOIN dim_crm_touchpoint
      ON fct_crm_touchpoint.dim_crm_touchpoint_id = dim_crm_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN dim_campaign
      ON fct_crm_touchpoint.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN fct_campaign
      ON fct_crm_touchpoint.dim_campaign_id = fct_campaign.dim_campaign_id
    LEFT JOIN dim_crm_person
      ON fct_crm_touchpoint.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN fct_crm_person
      ON fct_crm_touchpoint.dim_crm_person_id = fct_crm_person.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON fct_crm_touchpoint.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_sales_representative
      ON fct_campaign.campaign_owner_id = dim_crm_sales_representative.dim_crm_sales_rep_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-02-18",
    updated_date="2020-03-04"
) }}
