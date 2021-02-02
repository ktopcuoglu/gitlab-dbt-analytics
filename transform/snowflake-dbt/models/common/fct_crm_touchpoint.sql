WITH account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account') }}

), bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), final_touchpoint AS (

    SELECT
      touchpoint_id                             AS dim_crm_touchpoint_id,
      bizible_touchpoints.bizible_person_id,

      -- shared dimension keys
      crm_person.dim_crm_person_id,
      campaign_id                                       AS dim_campaign_id,
      account_dimensions.dim_account_crm_account_id,
      account_dimensions.dim_parent_crm_account_id,
      account_dimensions.dim_parent_sales_segment_id,
      account_dimensions.dim_parent_geo_region_id,
      account_dimensions.dim_parent_geo_sub_region_id,
      account_dimensions.dim_parent_geo_area_id,
      account_dimensions.dim_parent_sales_territory_id,
      account_dimensions.dim_parent_industry_id,
      account_dimensions.dim_parent_location_country_id,
      account_dimensions.dim_parent_location_region_id,
      account_dimensions.dim_account_sales_segment_id,
      account_dimensions.dim_account_geo_region_id,
      account_dimensions.dim_account_geo_sub_region_id,
      account_dimensions.dim_account_geo_area_id,
      account_dimensions.dim_account_sales_territory_id,
      account_dimensions.dim_account_industry_id,
      account_dimensions.dim_account_location_country_id,
      account_dimensions.dim_account_location_region_id,

      -- attribution counts
      bizible_count_first_touch,
      bizible_count_lead_creation_touch,
      bizible_count_u_shaped

    FROM bizible_touchpoints
    LEFT JOIN account_dimensions
      ON bizible_touchpoints.bizible_account = account_dimensions.dim_account_crm_account_id
    LEFT JOIN crm_person
      ON bizible_touchpoints.bizible_person_id = crm_person.bizible_person_id
)

{{ dbt_audit(
    cte_ref="final_touchpoint",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-21",
    updated_date="2021-02-02"
) }}
