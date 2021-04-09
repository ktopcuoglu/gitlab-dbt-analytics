WITH crm_user_hierarchy_live AS (

    SELECT
      dim_crm_user_hierarchy_live_id,
      dim_crm_user_sales_segment_id,
      crm_user_sales_segment,
      crm_user_sales_segment_grouped,
      dim_crm_user_geo_id,
      crm_user_geo,
      dim_crm_user_region_id,
      crm_user_region,
      dim_crm_user_area_id,
      crm_user_area,
      crm_user_sales_segment_region_grouped
    FROM {{ ref('prep_crm_user_hierarchy_live') }}
)

{{ dbt_audit(
    cte_ref="crm_user_hierarchy_live",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-04",
    updated_date="2021-03-26"
) }}
