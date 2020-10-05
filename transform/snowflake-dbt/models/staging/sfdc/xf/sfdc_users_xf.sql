with users as (

    SELECT * FROM {{ref('sfdc_users')}}

), user_role as (

    SELECT * FROM {{ref('sfdc_user_roles')}}

)

SELECT
  users.name            AS name,
  users.department      AS department,
  users.title           AS title,
  users.team,           --team,
  users.user_id,        --user_id
  manager.name          AS manager_name,
  manager.user_id       AS manager_id,
  user_role.name        AS role_name,
  users.start_date,      --start_date
  users.is_active        --is_active
FROM users
LEFT OUTER JOIN user_role 
ON users.user_role_id = user_role.id
LEFT OUTER JOIN users AS manager 
ON manager.user_id = users.manager_id
