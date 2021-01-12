WITH sfdc_user AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}
    WHERE is_active = 'TRUE'

), final_sales_hierarchy AS (

    SELECT DISTINCT

      {{ dbt_utils.surrogate_key(['CONCAT(user_segment,user_geo,user_region,user_area)']) }}   AS dim_crm_sales_hierarchy_live_id,
      {{ dbt_utils.surrogate_key(['user_segment']) }}                                          AS dim_crm_sales_hierarchy_sales_segment_live_id,
      user_segment                                                                             AS sales_segment_name_live,
      {{ dbt_utils.surrogate_key(['user_geo']) }}                                              AS dim_crm_sales_hierarchy_location_region_live_id,
      user_geo                                                                                 AS location_region_name_live,
      {{ dbt_utils.surrogate_key(['user_region']) }}                                           AS dim_crm_sales_hierarchy_sales_region_live_id,
      user_region                                                                              AS sales_region_name_live,
      {{ dbt_utils.surrogate_key(['user_area']) }}                                             AS dim_crm_sales_hierarchy_sales_area_live_id,
      user_area                                                                                AS sales_area_name_live

    FROM sfdc_user
    WHERE user_segment IS NOT NULL
      AND user_geo IS NOT NULL
      AND user_region IS NOT NULL
      AND user_area IS NOT NULL
      AND user_region <> 'Sales Admin'

)

{{ dbt_audit(
    cte_ref="final_sales_hierarchy",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2020-12-18"
) }}
