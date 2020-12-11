{{config({
    "schema": "common"
  })
}}

WITH sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source')}}

), sfdc_user_roles AS (

    SELECT *
    FROM {{ ref('sfdc_user_roles')}}

), final_users AS (

    SELECT
      sfdc_users.user_id              AS dim_crm_sales_rep_id,
      sfdc_users.name                 AS rep_name,
      sfdc_users.title,
      sfdc_users.department,
      sfdc_users.team,
      sfdc_users.manager_id,
      sfdc_users.is_active,
      sfdc_users.start_date,
      sfdc_users.user_role_id,
      sfdc_user_roles.name            AS user_role_name,
      sfdc_users.user_segment,
      sfdc_users.user_geo,
      sfdc_users.user_region,
      sfdc_users.user_area
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles
      ON sfdc_users.user_role_id = sfdc_user_roles.id

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-20",
    updated_date="2020-11-20"
) }}
