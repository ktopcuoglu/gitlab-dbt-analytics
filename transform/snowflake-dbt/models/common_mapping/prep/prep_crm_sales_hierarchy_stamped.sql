WITH sfdc_opportunity_sales_hierarchy AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE is_deleted = 'FALSE'

), final_sales_hierarchy_stamped AS (

    SELECT DISTINCT

      {{ dbt_utils.surrogate_key(['CONCAT(user_segment_stamped,user_geo_stamped,user_region_stamped,user_area_stamped)']) }}    AS dim_crm_sales_hierarchy_stamped_id,
      {{ dbt_utils.surrogate_key(['user_segment_stamped']) }}                                                                   AS dim_crm_sales_hierarchy_sales_segment_stamped_id,
      user_segment_stamped                                                                                                      AS sales_segment_name_stamped,
      {{ dbt_utils.surrogate_key(['user_geo_stamped']) }}                                                                       AS dim_crm_sales_hierarchy_location_region_stamped_id,
      user_geo_stamped                                                                                                          AS location_region_name_stamped,
      {{ dbt_utils.surrogate_key(['user_region_stamped']) }}                                                                    AS dim_crm_sales_hierarchy_sales_region_stamped_id,
      user_region_stamped                                                                                                       AS sales_region_name_stamped,
      {{ dbt_utils.surrogate_key(['user_area_stamped']) }}                                                                      AS dim_crm_sales_hierarchy_sales_area_stamped_id,
      user_area_stamped                                                                                                         AS sales_area_name_stamped


    FROM sfdc_opportunity_sales_hierarchy
    WHERE user_segment_stamped IS NOT NULL
      AND user_geo_stamped IS NOT NULL
      AND user_region_stamped IS NOT NULL
      AND user_area_stamped IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final_sales_hierarchy_stamped",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-05",
    updated_date="2021-01-05"
) }}
