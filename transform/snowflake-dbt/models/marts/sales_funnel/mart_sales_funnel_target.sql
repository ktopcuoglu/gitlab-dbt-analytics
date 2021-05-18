{{config({
    "schema": "common_mart_sales"
  })
}}

WITH dim_crm_user_hierarchy_live AS (

    SELECT *
    FROM {{ ref('dim_crm_user_hierarchy_live') }}

), dim_sales_qualified_source AS (

    SELECT *
    FROM {{ ref('dim_sales_qualified_source') }}

), dim_order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), fct_sales_funnel_target AS (

    SELECT *
    FROM {{ ref('fct_sales_funnel_target') }}

), final AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_target_id,
      fct_sales_funnel_target.first_day_of_month      AS target_month,
      fct_sales_funnel_target.kpi_name,
      dim_crm_user_hierarchy_live.crm_user_sales_segment,
      dim_crm_user_hierarchy_live.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy_live.crm_user_geo,
      dim_crm_user_hierarchy_live.crm_user_region,
      dim_crm_user_hierarchy_live.crm_user_area,
      dim_crm_user_hierarchy_live.crm_user_sales_segment_region_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sales_qualified_source_grouped,
      fct_sales_funnel_target.allocated_target
    FROM fct_sales_funnel_target
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_target.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy_live
      ON fct_sales_funnel_target.dim_crm_user_sales_segment_id = dim_crm_user_hierarchy_live.dim_crm_user_sales_segment_id
      AND fct_sales_funnel_target.dim_crm_user_geo_id = dim_crm_user_hierarchy_live.dim_crm_user_geo_id
      AND fct_sales_funnel_target.dim_crm_user_region_id = dim_crm_user_hierarchy_live.dim_crm_user_region_id
      AND fct_sales_funnel_target.dim_crm_user_area_id = dim_crm_user_hierarchy_live.dim_crm_user_area_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2021-01-08",
    updated_date="2021-04-26",
  ) }}
