{{ simple_cte([
      ('dim_crm_user_hierarchy_live', 'dim_crm_user_hierarchy_live'),
      ('dim_crm_user_hierarchy_stamped', 'dim_crm_user_hierarchy_stamped'),
      ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
      ('dim_order_type', 'dim_order_type'),
      ('fct_sales_funnel_target', 'fct_sales_funnel_target')
])}}

, final AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_target_id,
      fct_sales_funnel_target.first_day_of_month                                                                                                                            AS target_month,
      fct_sales_funnel_target.kpi_name,
      COALESCE(dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped,dim_crm_user_hierarchy_live.crm_user_sales_segment)                                       AS crm_user_sales_segment,
      COALESCE(dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped_grouped,dim_crm_user_hierarchy_live.crm_user_sales_segment_grouped)                       AS crm_user_sales_segment_grouped,
      COALESCE(dim_crm_user_hierarchy_stamped.crm_opp_owner_geo_stamped,dim_crm_user_hierarchy_live.crm_user_geo)                                                           AS crm_user_geo,
      COALESCE(dim_crm_user_hierarchy_stamped.crm_opp_owner_region_stamped,dim_crm_user_hierarchy_live.crm_user_region)                                                     AS crm_user_region,
      COALESCE(dim_crm_user_hierarchy_stamped.crm_opp_owner_area_stamped,dim_crm_user_hierarchy_live.crm_user_area)                                                         AS crm_user_area,
      COALESCE(dim_crm_user_hierarchy_live.crm_user_sales_segment_region_grouped, dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_region_stamped_grouped)        AS crm_user_sales_segment_region_grouped,
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
    LEFT JOIN dim_crm_user_hierarchy_stamped
      ON fct_sales_funnel_target.crm_user_sales_segment_geo_region_area = dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_geo_region_area_stamped
        AND fct_sales_funnel_target.fiscal_year = dim_crm_user_hierarchy_stamped.fiscal_year
    LEFT JOIN dim_crm_user_hierarchy_live
      ON fct_sales_funnel_target.crm_user_sales_segment_geo_region_area = dim_crm_user_hierarchy_live.crm_user_sales_segment_geo_region_area

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2021-01-08",
    updated_date="2022-03-07",
  ) }}
