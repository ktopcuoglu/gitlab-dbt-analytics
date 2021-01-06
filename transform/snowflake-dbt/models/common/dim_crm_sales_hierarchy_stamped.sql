WITH crm_sales_hierarchy_stamped AS (

    SELECT
      dim_crm_sales_hierarchy_stamped_id,
      crm_sales_hierarchy_stamped,
      user_segment_stamped,
      dim_sales_segment_stamped_id,
      location_region_stamped,
      dim_location_region_stamped_id,
      user_region_stamped,
      dim_sales_region_stamped_id,
      user_area_stamped,
      dim_sales_area_stamped_id
    FROM {{ ref('prep_crm_sales_hierarchy_stamped') }}
)

{{ dbt_audit(
    cte_ref="crm_sales_hierarchy_stamped",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-05",
    updated_date="2021-01-06"
) }}
