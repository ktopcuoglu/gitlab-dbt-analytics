WITH crm_sales_hierarchy_live AS (

    SELECT
      dim_crm_sales_hierarchy_live_id,
      dim_crm_sales_hierarchy_sales_segment_live_id,
      sales_segment_name_live,
      dim_crm_sales_hierarchy_location_region_live_id,
      location_region_name_live,
      dim_crm_sales_hierarchy_sales_region_live_id,
      sales_region_name_live,
      dim_crm_sales_hierarchy_sales_area_live_id,
      sales_area_name_live
    FROM {{ ref('prep_crm_sales_hierarchy_live') }}
)

{{ dbt_audit(
    cte_ref="crm_sales_hierarchy_live",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-04",
    updated_date="2021-01-06"
) }}
