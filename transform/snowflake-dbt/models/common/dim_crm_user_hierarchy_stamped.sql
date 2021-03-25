WITH crm_user_hierarchy_stamped AS (

    SELECT
      dim_crm_user_hierarchy_stamped_id,
      dim_crm_opp_owner_sales_segment_stamped_id,
      crm_opp_owner_sales_segment_stamped,
      dim_crm_opp_owner_geo_stamped_id,
      crm_opp_owner_geo_stamped,
      dim_crm_opp_owner_region_stamped_id,
      crm_opp_owner_region_stamped,
      dim_crm_opp_owner_area_stamped_id,
      crm_opp_owner_area_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}
)

{{ dbt_audit(
    cte_ref="crm_user_hierarchy_stamped",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-05",
    updated_date="2021-03-25"
) }}
