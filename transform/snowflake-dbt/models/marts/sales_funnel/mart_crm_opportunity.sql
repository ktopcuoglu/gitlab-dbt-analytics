{{config({
    "schema": "common_mart_sales"
  })
}}

{{ simple_cte([
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_opportunity','dim_crm_opportunity'),
    ('dim_sales_qualified_source','dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('dim_deal_path','dim_deal_path'),
    ('fct_crm_opportunity','fct_crm_opportunity')
]) }}

, dim_crm_user_hierarchy_live_sales_segment AS (

    SELECT DISTINCT
      dim_crm_user_sales_segment_id,
      crm_user_sales_segment
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_geo AS (

    SELECT DISTINCT
      dim_crm_user_geo_id,
      crm_user_geo
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_region AS (

    SELECT DISTINCT
      dim_crm_user_region_id,
      crm_user_region
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_live_area AS (

    SELECT DISTINCT
      dim_crm_user_area_id,
      crm_user_area
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_crm_user_hierarchy_stamped_sales_segment AS (

    SELECT DISTINCT
      dim_crm_opp_owner_sales_segment_stamped_id,
      crm_opp_owner_sales_segment_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_geo AS (

    SELECT DISTINCT
      dim_crm_opp_owner_geo_stamped_id,
      crm_opp_owner_geo_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_region AS (

    SELECT DISTINCT
      dim_crm_opp_owner_region_stamped_id,
      crm_opp_owner_region_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), dim_crm_user_hierarchy_stamped_area AS (

    SELECT DISTINCT
      dim_crm_opp_owner_area_stamped_id,
      crm_opp_owner_area_stamped
    FROM {{ ref('dim_crm_user_hierarchy_stamped') }}

), final AS (

    SELECT
      fct_crm_opportunity.sales_accepted_date,
      DATE_TRUNC(month, fct_crm_opportunity.sales_accepted_date)           AS sales_accepted_month,
      fct_crm_opportunity.close_date,
      DATE_TRUNC(month, fct_crm_opportunity.close_date)                    AS close_month,
      fct_crm_opportunity.created_date,
      DATE_TRUNC(month, fct_crm_opportunity.created_date)                  AS created_month,
      fct_crm_opportunity.dim_crm_opportunity_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.dim_crm_account_id,

      -- opportunity attributes & additive fields
      fct_crm_opportunity.is_won,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.days_in_sao,
      fct_crm_opportunity.arr_basis,
      fct_crm_opportunity.iacv,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.amount,
      dim_crm_opportunity.is_edu_oss,
      dim_crm_opportunity.is_ps_opp,
      dim_crm_opportunity.stage_name,
      dim_crm_opportunity.reason_for_loss,
      dim_crm_opportunity.sales_type,
      fct_crm_opportunity.is_sao,
      dim_deal_path.deal_path_name,
      dim_order_type.order_type_name                                       AS order_type,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.parent_crm_account_sales_segment,
      fct_crm_opportunity.closed_buckets,
      dim_crm_opportunity.opportunity_category,
      dim_crm_opportunity.source_buckets,
      dim_crm_opportunity.opportunity_sales_development_representative,
      dim_crm_opportunity.opportunity_business_development_representative,
      dim_crm_opportunity.opportunity_development_representative,
      dim_crm_opportunity.is_web_portal_purchase,
      dim_crm_opportunity.sales_path,
      dim_crm_opportunity.professional_services_value,
      fct_crm_opportunity.primary_solution_architect,
      fct_crm_opportunity.product_details,
      fct_crm_opportunity.product_category,
      fct_crm_opportunity.products_purchased,
      
      -- crm opp owner/account owner fields stamped at SAO date
      dim_crm_opportunity.sao_crm_opp_owner_stamped_name,
      dim_crm_opportunity.sao_crm_account_owner_stamped_name,
      dim_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      dim_crm_opportunity.sao_crm_opp_owner_geo_stamped,
      dim_crm_opportunity.sao_crm_opp_owner_region_stamped,
      dim_crm_opportunity.sao_crm_opp_owner_area_stamped,
      
      -- crm opp owner/account owner stamped fields stamped at close date
      dim_crm_opportunity.crm_opp_owner_stamped_name,
      dim_crm_opportunity.crm_account_owner_stamped_name,
      dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped,
      dim_crm_user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped,
      dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped,
      dim_crm_user_hierarchy_stamped_area.crm_opp_owner_area_stamped,
      {{ sales_segment_region_grouped('dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped',
        'dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped') }}
                                                                           AS crm_opp_owner_sales_segment_region_stamped_grouped,
      
      -- crm owner/sales rep live fields
      dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment,
      dim_crm_user_hierarchy_live_geo.crm_user_geo,
      dim_crm_user_hierarchy_live_region.crm_user_region,
      dim_crm_user_hierarchy_live_area.crm_user_area,
      {{ sales_segment_region_grouped('dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment',
        'dim_crm_user_hierarchy_live_region.crm_user_region') }}
                                                                           AS crm_user_sales_segment_region_grouped,

      -- channel fields
      fct_crm_opportunity.lead_source,
      fct_crm_opportunity.dr_partner_deal_type,
      fct_crm_opportunity.dr_partner_engagement,
      fct_crm_opportunity.partner_account,
      fct_crm_opportunity.dr_status,
      fct_crm_opportunity.distributor,
      fct_crm_opportunity.influence_partner,
      fct_crm_opportunity.fulfillment_partner,
      fct_crm_opportunity.platform_partner,
      fct_crm_opportunity.partner_track,
      fct_crm_opportunity.is_public_sector_opp,
      fct_crm_opportunity.is_registration_from_portal,
      fct_crm_opportunity.calculated_discount,
      fct_crm_opportunity.partner_discount,
      fct_crm_opportunity.partner_discount_calc,
      fct_crm_opportunity.comp_channel_neutral,
      fct_crm_opportunity.count_crm_attribution_touchpoints,
      fct_crm_opportunity.weighted_linear_iacv,
      fct_crm_opportunity.count_campaigns

    FROM fct_crm_opportunity
    LEFT JOIN dim_crm_opportunity
      ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN dim_crm_account
      ON dim_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_sales_qualified_source
      ON fct_crm_opportunity.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_deal_path
      ON fct_crm_opportunity.dim_deal_path_id = dim_deal_path.dim_deal_path_id
    LEFT JOIN dim_order_type
      ON fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_sales_segment
      ON fct_crm_opportunity.dim_crm_opp_owner_sales_segment_stamped_id = dim_crm_user_hierarchy_stamped_sales_segment.dim_crm_opp_owner_sales_segment_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_geo
      ON fct_crm_opportunity.dim_crm_opp_owner_geo_stamped_id = dim_crm_user_hierarchy_stamped_geo.dim_crm_opp_owner_geo_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_region
      ON fct_crm_opportunity.dim_crm_opp_owner_region_stamped_id = dim_crm_user_hierarchy_stamped_region.dim_crm_opp_owner_region_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_stamped_area
      ON fct_crm_opportunity.dim_crm_opp_owner_area_stamped_id = dim_crm_user_hierarchy_stamped_area.dim_crm_opp_owner_area_stamped_id
    LEFT JOIN dim_crm_user_hierarchy_live_sales_segment
      ON fct_crm_opportunity.dim_crm_user_sales_segment_id = dim_crm_user_hierarchy_live_sales_segment.dim_crm_user_sales_segment_id
    LEFT JOIN dim_crm_user_hierarchy_live_geo
      ON fct_crm_opportunity.dim_crm_user_geo_id = dim_crm_user_hierarchy_live_geo.dim_crm_user_geo_id
    LEFT JOIN dim_crm_user_hierarchy_live_region
      ON fct_crm_opportunity.dim_crm_user_region_id = dim_crm_user_hierarchy_live_region.dim_crm_user_region_id
    LEFT JOIN dim_crm_user_hierarchy_live_area
      ON fct_crm_opportunity.dim_crm_user_area_id = dim_crm_user_hierarchy_live_area.dim_crm_user_area_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2020-12-07",
    updated_date="2021-04-22",
  ) }}
