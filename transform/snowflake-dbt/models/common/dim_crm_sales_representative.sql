WITH sfdc_users AS (

    SELECT *
    FROM {{ ref('prep_crm_sales_representative')}}

), final_users AS (

    SELECT

      dim_crm_sales_rep_id,
      rep_name,
      title,
      department,
      team,
      manager_id,
      is_active,
      start_date,
      user_role_id,
      user_role_name,
      sales_segment_name_live,
      location_region_name_live,
      sales_region_name_live,
      sales_area_name_live

    FROM sfdc_users

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-20",
    updated_date="2020-12-11"
) }}
