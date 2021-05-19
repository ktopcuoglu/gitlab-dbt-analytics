WITH sfdc_user AS (

    SELECT *
    FROM {{ ref('prep_crm_user') }}
    WHERE is_active = 'TRUE'

), final_sales_hierarchy AS (

    SELECT DISTINCT

      {{ dbt_utils.surrogate_key(['crm_user_sales_segment', 'crm_user_geo', 'crm_user_region', 'crm_user_area']) }}   AS dim_crm_user_hierarchy_live_id,
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

    FROM sfdc_user
    WHERE crm_user_sales_segment IS NOT NULL
      AND crm_user_geo IS NOT NULL
      AND crm_user_region IS NOT NULL
      AND crm_user_area IS NOT NULL
      AND crm_user_region <> 'Sales Admin'

)

{{ dbt_audit(
    cte_ref="final_sales_hierarchy",
    created_by="@mcooperDD",
    updated_by="@iweeks",
    created_date="2020-12-18",
    updated_date="2021-04-22"
) }}
