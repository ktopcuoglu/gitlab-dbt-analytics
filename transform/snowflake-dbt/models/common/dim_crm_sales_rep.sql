{{config({
    "schema": "legacy"
  })
}}

WITH sfdc_users AS (

  SELECT *
  FROM {{ ref('sfdc_users')}}

), sfdc_user_roles AS (

  SELECT *
  FROM {{ ref('sfdc_user_roles')}}

), joined AS (

  SELECT 

    sfdc_users.user_id     AS crm_sales_rep_id,
    sfdc_users.name        AS rep_name,
    sfdc_users.title       AS title,
    sfdc_users.department, -- department
    sfdc_users.team,       -- team
    sfdc_users.manager_id, -- manager_id
    sfdc_users.is_active,  -- is_active
    sfdc_users.start_date, -- start_date
    sfdc_user_roles.name   AS user_role_name
	
  FROM sfdc_users
  LEFT JOIN sfdc_user_roles
    ON sfdc_users.user_role_id = sfdc_user_roles.id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@jjstark",
    updated_by="@jjstark",
    created_date="2020-09-29",
    updated_date="2020-09-29"
) }}
