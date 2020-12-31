WITH sfdc_user AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}
    WHERE is_active = 'TRUE'

), final_sales_hierarchy AS (

    SELECT DISTINCT

    CONCAT(user_segment,'-',user_geo,'-',user_region,'-',user_area)   AS sales_hierarchy,
    {{ dbt_utils.surrogate_key(['sales_hierarchy']) }}                AS dim_sales_hierarchy_id,
    user_segment,
    {{ dbt_utils.surrogate_key(['user_segment']) }}                   AS dim_sales_segment_id,
    user_geo,
    {{ dbt_utils.surrogate_key(['user_geo']) }}                       AS dim_location_region_id,
    user_region,
    {{ dbt_utils.surrogate_key(['user_region']) }}                    AS dim_sales_region_id,
    user_area,
    {{ dbt_utils.surrogate_key(['user_area']) }}                      AS dim_sales_area_id

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
