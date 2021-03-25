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
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area

    FROM sfdc_users

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-20",
    updated_date="2021-03-25"
) }}
