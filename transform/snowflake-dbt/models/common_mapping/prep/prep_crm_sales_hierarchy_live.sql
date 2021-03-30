WITH sfdc_user AS (

    SELECT *
    FROM {{ ref('prep_crm_sales_representative') }}
    WHERE is_active = 'TRUE'

), final_sales_hierarchy AS (

    SELECT DISTINCT

      {{ dbt_utils.surrogate_key(['sales_segment_name_live', 'location_region_name_live', 'sales_region_name_live', 'sales_area_name_live']) }}   AS dim_crm_sales_hierarchy_live_id,
      dim_crm_sales_hierarchy_sales_segment_live_id,
      sales_segment_name_live,
      sales_segment_name_live_grouped,
      dim_crm_sales_hierarchy_location_region_live_id,
      location_region_name_live,
      dim_crm_sales_hierarchy_sales_region_live_id,
      sales_region_name_live,
      segment_region_live_grouped,
      dim_crm_sales_hierarchy_sales_area_live_id,
      sales_area_name_live

    FROM sfdc_user
    WHERE sales_segment_name_live IS NOT NULL
      AND location_region_name_live IS NOT NULL
      AND sales_region_name_live IS NOT NULL
      AND sales_area_name_live IS NOT NULL
      AND sales_region_name_live <> 'Sales Admin'

)

{{ dbt_audit(
    cte_ref="final_sales_hierarchy",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-12-18",
    updated_date="2021-03-24"
) }}
