WITH date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     first_day_of_month
   FROM {{ ref('date_details_source') }}

), sales_qualified_source AS (

    SELECT *
    FROM {{ ref('prep_sales_qualified_source') }}

), order_type AS (

    SELECT *
    FROM {{ ref('prep_order_type') }}

), sfdc_user_hierarchy_live AS (

    SELECT *
    FROM {{ ref('prep_crm_sales_hierarchy_live') }}

), sfdc_user_hierarchy_stamped AS (

    SELECT *
    FROM {{ ref('prep_crm_sales_hierarchy_stamped') }}

), target_matrix AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source' )}}

), final_targets AS (

  SELECT

    {{ dbt_utils.surrogate_key(['target_matrix.kpi_name', 'date.first_day_of_month', 'sales_qualified_source.dim_sales_qualified_source_id',
           'order_type.dim_order_type_id', 'sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_live_id', 'sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_segment_live_id',
           'sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_location_region_live_id', 'sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_region_live_id', 'sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_area_live_id']) }}
                                            AS sales_funnel_target_id,
    target_matrix.kpi_name,
    date.first_day_of_month,
    target_matrix.opportunity_source        AS sales_qualified_source,
    sales_qualified_source.dim_sales_qualified_source_id,
    target_matrix.order_type,
    order_type.dim_order_type_id,
    sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_live_id,
    sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_segment_live_id,
    sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_location_region_live_id,
    sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_region_live_id,
    sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_sales_area_live_id,
    sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_sales_segment_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_location_region_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_sales_region_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_sales_area_stamped_id,
    SUM(target_matrix.allocated_target)     AS allocated_target

  FROM target_matrix
  LEFT JOIN sfdc_user_hierarchy_live
    ON {{ sales_funnel_text_slugify("target_matrix.area") }} = {{ sales_funnel_text_slugify("sfdc_user_hierarchy_live.sales_area_name_live") }}
  LEFT JOIN date
    ON {{ sales_funnel_text_slugify("target_matrix.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
  LEFT JOIN sales_qualified_source
    ON {{ sales_funnel_text_slugify("target_matrix.opportunity_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
  LEFT JOIN order_type
    ON {{ sales_funnel_text_slugify("target_matrix.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
  LEFT JOIN sfdc_user_hierarchy_stamped
    ON sfdc_user_hierarchy_live.dim_crm_sales_hierarchy_live_id = sfdc_user_hierarchy_stamped.dim_crm_sales_hierarchy_stamped_id
  {{ dbt_utils.group_by(n=17) }}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2020-02-26"
) }}
