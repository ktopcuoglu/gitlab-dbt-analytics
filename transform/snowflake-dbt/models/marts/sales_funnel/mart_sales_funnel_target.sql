{{config({
    "schema": "common_mart_sales"
  })
}}

WITH dim_crm_sales_hierarchy_stamped AS (

    SELECT *
    FROM {{ ref('dim_crm_sales_hierarchy_stamped') }}

), dim_opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source') }}

), dim_order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), fct_sales_funnel_target AS (

    SELECT *
    FROM {{ ref('fct_sales_funnel_target') }}

), final AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_target_id,
      fct_sales_funnel_target.first_day_of_month AS target_month,
      fct_sales_funnel_target.kpi_name,
      dim_crm_sales_hierarchy_stamped.sales_segment_name_stamped,
      dim_crm_sales_hierarchy_stamped.location_region_name_stamped,
      dim_crm_sales_hierarchy_stamped.sales_region_name_stamped,
      dim_crm_sales_hierarchy_stamped.sales_area_name_stamped,
      dim_order_type.order_type_name,
      dim_opportunity_source.opportunity_source_name,
      fct_sales_funnel_target.allocated_target,
      fct_sales_funnel_target.kpi_total
    FROM fct_sales_funnel_target
    LEFT JOIN dim_opportunity_source
      ON fct_sales_funnel_target.dim_opportunity_source_id = dim_opportunity_source.dim_opportunity_source_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_sales_hierarchy_stamped
      ON fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_segment_stamped_id = dim_crm_sales_hierarchy_stamped.dim_crm_sales_hierarchy_sales_segment_stamped_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_location_region_stamped_id = dim_crm_sales_hierarchy_stamped.dim_crm_sales_hierarchy_location_region_stamped_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_region_stamped_id = dim_crm_sales_hierarchy_stamped.dim_crm_sales_hierarchy_sales_region_stamped_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_area_stamped_id = dim_crm_sales_hierarchy_stamped.dim_crm_sales_hierarchy_sales_area_stamped_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-01-08",
    updated_date="2021-01-08",
  ) }}
