WITH sfdc_user AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}
    WHERE is_active = 'TRUE'

), final_sales_hierarchy AS (

    SELECT DISTINCT

    CONCAT(user_segment,'-',user_geo,'-',user_region,'-',user_area)   AS crm_sales_hierarchy_live,
    {{ dbt_utils.surrogate_key(['crm_sales_hierarchy_live']) }}       AS dim_crm_sales_hierarchy_live_id,
    user_segment                                                      AS user_segment_live,
    {{ dbt_utils.surrogate_key(['user_segment']) }}                   AS dim_sales_segment_live_id,
    user_geo                                                          AS user_geo_live,
    {{ dbt_utils.surrogate_key(['user_geo']) }}                       AS dim_location_region_live_id,
    user_region                                                       AS location_region_live,
    {{ dbt_utils.surrogate_key(['user_region']) }}                    AS dim_sales_region_live_id,
    user_area                                                         AS user_area_live,
    {{ dbt_utils.surrogate_key(['user_area']) }}                      AS dim_sales_area_live_id

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
