WITH crm_sales_hierarchy_stamped AS (

    SELECT
      dim_crm_sales_hierarchy_stamped_id,
      dim_crm_sales_hierarchy_sales_segment_stamped_id,
      sales_segment_name_stamped,
      dim_crm_sales_hierarchy_location_region_stamped_id,
      location_region_name_stamped,
      dim_crm_sales_hierarchy_sales_region_stamped_id,
      sales_region_name_stamped,
      dim_crm_sales_hierarchy_sales_area_stamped_id,
      sales_area_name_stamped
    FROM {{ ref('prep_crm_sales_hierarchy_stamped') }}
)

{{ dbt_audit(
    cte_ref="crm_sales_hierarchy_stamped",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-05",
    updated_date="2021-01-06"
) }}
