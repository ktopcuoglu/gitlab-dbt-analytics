WITH crm_sales_hierarchy_live AS (

    SELECT
      crm_sales_hierarchy_live,
      dim_crm_sales_hierarchy_live_id,
      user_segment_live,
      dim_sales_segment_live_id,
      user_geo_live,
      dim_location_region_live_id,
      location_region_live,
      dim_sales_region_live_id,
      user_area_live,
      dim_sales_area_live_id
    FROM {{ ref('prep_crm_sales_hierarchy_live') }}
)

{{ dbt_audit(
    cte_ref="crm_sales_hierarchy_live",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-11-04",
    updated_date="2020-12-18"
) }}
