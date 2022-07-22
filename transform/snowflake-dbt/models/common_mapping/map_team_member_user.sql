WITH employees AS (
  -- Attempts to get the most recent username for a team member
  SELECT DISTINCT
    employee_id,
    LAST_VALUE(gitlab_username) OVER 
      (PARTITION BY employee_id ORDER BY uploaded_at ASC) AS gitlab_username
  FROM {{ ref('blended_employee_mapping_source') }} 
  WHERE NULLIF(gitlab_username, '') IS NOT NULL

),

users AS (
  -- Attempts to get the most recent user id for a username
  SELECT
    user_id,
    user_name
  FROM {{ ref('gitlab_dotcom_users_source') }} 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY last_activity_on DESC ) = 1

),

map AS (

  SELECT
    {{ dbt_utils.surrogate_key(['employees.employee_id']) }} AS dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['users.user_id']) }} AS dim_user_sk,
    employees.employee_id,
    users.user_id,
    users.user_name AS gitlab_username
  FROM employees
  LEFT JOIN users
    ON employees.gitlab_username = users.user_name
  WHERE users.user_id IS NOT NULL
)

{{ dbt_audit(
    cte_ref="map",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2022-07-22",
    updated_date="2022-07-22"
) }}

