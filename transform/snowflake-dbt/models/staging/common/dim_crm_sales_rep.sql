WITH sfdc_users AS (

  SELECT *
  FROM {{ ref('sfdc_users')}}

), sfdc_user_roles AS (

  SELECT *
  FROM {{ ref('sfdc_user_roles')}}

), joined AS (

  SELECT 

    sfdc_users.*,
		sfdc_user_roles.name AS user_role_name
	
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