WITH sfdc_opportunity_sales_hierarchy AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE is_deleted = 'FALSE'

), final_sales_hierarchy_stamped AS (

    SELECT DISTINCT

      {{ dbt_utils.surrogate_key(['CONCAT(user_segment_stamped,user_geo_stamped,user_region_stamped,user_area_stamped)']) }}    AS dim_crm_user_hierarchy_stamped_id,
      {{ dbt_utils.surrogate_key(['user_segment_stamped']) }}                                                                   AS dim_crm_opp_owner_sales_segment_stamped_id,
      user_segment_stamped                                                                                                      AS crm_opp_owner_sales_segment_stamped,
      user_segment_stamped_grouped                                                                                              AS crm_opp_owner_sales_segment_stamped_grouped,
      {{ dbt_utils.surrogate_key(['user_geo_stamped']) }}                                                                       AS dim_crm_opp_owner_geo_stamped_id,
      user_geo_stamped                                                                                                          AS crm_opp_owner_geo_stamped,
      {{ dbt_utils.surrogate_key(['user_region_stamped']) }}                                                                    AS dim_crm_opp_owner_region_stamped_id,
      user_region_stamped                                                                                                       AS crm_opp_owner_region_stamped,
      user_segment_region_stamped_grouped                                                                                       AS crm_opp_owner_sales_segment_region_stamped_grouped,
      {{ dbt_utils.surrogate_key(['user_area_stamped']) }}                                                                      AS dim_crm_opp_owner_area_stamped_id,
      user_area_stamped                                                                                                         AS crm_opp_owner_area_stamped


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
    updated_date="2021-03-26"
) }}
