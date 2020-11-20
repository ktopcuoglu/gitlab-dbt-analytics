{{config({
    "schema": "common"
  })
}}

WITH sfdc_users AS (

    SELECT *
    FROM "ANALYTICS"."SFDC"."SFDC_USERS_SOURCE"

), sfdc_user_roles AS (

    SELECT *
    FROM "ANALYTICS"."SFDC"."SFDC_USER_ROLES_SOURCE"

), joined AS (

    SELECT

      sfdc_users.user_id              AS crm_sales_rep_id,
      sfdc_users.name                 AS rep_name,
      sfdc_users.title                AS title,
      sfdc_users.department,          -- department
      sfdc_users.team,                -- team
      sfdc_users.manager_id,          -- manager_id
      sfdc_users.is_active,           -- is_active
      sfdc_users.start_date,          -- start_date
      sfdc_users.user_role_id,        -- user_role_id
      sfdc_user_roles.name            AS user_role_name,
      sfdc_user_roles.parentroleid    AS parent_role_id

    FROM sfdc_users
    LEFT JOIN sfdc_user_roles
      ON sfdc_users.user_role_id = sfdc_user_roles.id

), sales_rep_hierarchy AS (

    SELECT
      id,
      sys_connect_by_path(name, ' :: ')   AS path,
      level
    FROM sfdc_user_roles
    START WITH name = 'CRO'
    CONNECT BY PRIOR id = parentroleid

), final_users AS (

    SELECT
      crm_sales_rep_id,
      rep_name,
      title,
      department,
      team,
      manager_id,
      is_active,
      start_date,
      user_role_name,
      SPLIT_PART(sales_rep_hierarchy.path, '::', 2)::VARCHAR AS parent_role_1,
      SPLIT_PART(sales_rep_hierarchy.path, '::', 3)::VARCHAR AS parent_role_2,
      SPLIT_PART(sales_rep_hierarchy.path, '::', 4)::VARCHAR AS parent_role_3,
      SPLIT_PART(sales_rep_hierarchy.path, '::', 5)::VARCHAR AS parent_role_4,
      SPLIT_PART(sales_rep_hierarchy.path, '::', 6)::VARCHAR AS parent_role_5
    FROM joined
    LEFT JOIN sales_rep_hierarchy
      ON joined.user_role_id = sales_rep_hierarchy.id
)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-20",
    updated_date="2020-11-20"
) }}
