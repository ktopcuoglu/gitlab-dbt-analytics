WITH crm_sales_hierarchy_live AS (

    SELECT
      dim_crm_sales_hierarchy_live_id,
      crm_sales_hierarchy_live,
      user_segment_live,
      dim_sales_segment_live_id,
      location_region_live,
      dim_location_region_live_id,
      user_region_live,
      dim_sales_region_live_id,
      user_area_live,
      dim_sales_area_live_id
    FROM {{ ref('prep_crm_sales_hierarchy_live') }}
)

{{ dbt_audit(
    cte_ref="crm_sales_hierarchy_live",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-04",
    updated_date="2021-01-06"
) }}
