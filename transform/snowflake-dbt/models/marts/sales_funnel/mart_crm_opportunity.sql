{{config({
    "schema": "common_mart_sales"
  })
}}

WITH dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_crm_opportunity AS (

    SELECT *
    FROM {{ ref('dim_crm_opportunity') }}

), dim_opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source') }}

), dim_order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), dim_purchase_channel AS (

    SELECT *
    FROM {{ ref('dim_purchase_channel') }}

), dim_crm_sales_hierarchy_live_sales_segment AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_segment_live_id,
      sales_segment_name_live
    FROM {{ ref('dim_crm_sales_hierarchy_live') }}

), dim_crm_sales_hierarchy_live_location_region AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_location_region_live_id,
      location_region_name_live
    FROM {{ ref('dim_crm_sales_hierarchy_live') }}

), dim_crm_sales_hierarchy_live_sales_region AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_region_live_id,
      sales_region_name_live
    FROM {{ ref('dim_crm_sales_hierarchy_live') }}

), dim_crm_sales_hierarchy_live_sales_area AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_area_live_id,
      sales_area_name_live
    FROM {{ ref('dim_crm_sales_hierarchy_live') }}

), dim_crm_sales_hierarchy_stamped_sales_segment AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_segment_stamped_id,
      sales_segment_name_stamped
    FROM {{ ref('dim_crm_sales_hierarchy_stamped') }}

), dim_crm_sales_hierarchy_stamped_location_region AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_location_region_stamped_id,
      location_region_name_stamped
    FROM {{ ref('dim_crm_sales_hierarchy_stamped') }}

), dim_crm_sales_hierarchy_stamped_sales_region AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_region_stamped_id,
      sales_region_name_stamped
    FROM {{ ref('dim_crm_sales_hierarchy_stamped') }}

), dim_crm_sales_hierarchy_stamped_sales_area AS (

    SELECT DISTINCT
      dim_crm_sales_hierarchy_sales_area_stamped_id,
      sales_area_name_stamped
    FROM {{ ref('dim_crm_sales_hierarchy_stamped') }}

), fct_crm_opportunity AS (

    SELECT *
    FROM {{ ref('fct_crm_opportunity') }}

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
      fct_crm_opportunity.is_won,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.days_in_sao,
      fct_crm_opportunity.iacv,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.amount,
      dim_crm_opportunity.is_edu_oss,
      dim_crm_opportunity.stage_name,
      dim_crm_opportunity.reason_for_loss,
      fct_crm_opportunity.is_sao,
      dim_crm_sales_hierarchy_stamped_sales_segment.sales_segment_name_stamped,
      dim_crm_sales_hierarchy_stamped_location_region.location_region_name_stamped,
      dim_crm_sales_hierarchy_stamped_sales_region.sales_region_name_stamped,
      dim_crm_sales_hierarchy_stamped_sales_area.sales_area_name_stamped,
      dim_crm_sales_hierarchy_live_sales_segment.sales_segment_name_live,
      dim_crm_sales_hierarchy_live_location_region.location_region_name_live,
      dim_crm_sales_hierarchy_live_sales_region.sales_region_name_live,
      dim_crm_sales_hierarchy_live_sales_area.sales_area_name_live,
      dim_purchase_channel.purchase_channel_name,
      dim_order_type.order_type_name                                       AS order_type,
      dim_opportunity_source.opportunity_source_name,
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
      dim_crm_opportunity.is_web_portal_purchase
    FROM fct_crm_opportunity
    LEFT JOIN dim_crm_opportunity
      ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN dim_crm_account
      ON dim_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_opportunity_source
      ON fct_crm_opportunity.dim_opportunity_source_id = dim_opportunity_source.dim_opportunity_source_id
    LEFT JOIN dim_purchase_channel
      ON fct_crm_opportunity.dim_purchase_channel_id = dim_purchase_channel.dim_purchase_channel_id
    LEFT JOIN dim_order_type
      ON fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_sales_hierarchy_stamped_sales_segment
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_segment_stamped_id = dim_crm_sales_hierarchy_stamped_sales_segment.dim_crm_sales_hierarchy_sales_segment_stamped_id
    LEFT JOIN dim_crm_sales_hierarchy_stamped_location_region
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_location_region_stamped_id = dim_crm_sales_hierarchy_stamped_location_region.dim_crm_sales_hierarchy_location_region_stamped_id
    LEFT JOIN dim_crm_sales_hierarchy_stamped_sales_region
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_region_stamped_id = dim_crm_sales_hierarchy_stamped_sales_region.dim_crm_sales_hierarchy_sales_region_stamped_id
    LEFT JOIN dim_crm_sales_hierarchy_stamped_sales_area
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_area_stamped_id = dim_crm_sales_hierarchy_stamped_sales_area.dim_crm_sales_hierarchy_sales_area_stamped_id
    LEFT JOIN dim_crm_sales_hierarchy_live_sales_segment
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_segment_live_id = dim_crm_sales_hierarchy_live_sales_segment.dim_crm_sales_hierarchy_sales_segment_live_id
    LEFT JOIN dim_crm_sales_hierarchy_live_location_region
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_location_region_live_id = dim_crm_sales_hierarchy_live_location_region.dim_crm_sales_hierarchy_location_region_live_id
    LEFT JOIN dim_crm_sales_hierarchy_live_sales_region
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_region_live_id = dim_crm_sales_hierarchy_live_sales_region.dim_crm_sales_hierarchy_sales_region_live_id
    LEFT JOIN dim_crm_sales_hierarchy_live_sales_area
      ON fct_crm_opportunity.dim_crm_sales_hierarchy_sales_area_live_id = dim_crm_sales_hierarchy_live_sales_area.dim_crm_sales_hierarchy_sales_area_live_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@mcooperDD",
    created_date="2020-12-07",
    updated_date="2021-02-17",
  ) }}
