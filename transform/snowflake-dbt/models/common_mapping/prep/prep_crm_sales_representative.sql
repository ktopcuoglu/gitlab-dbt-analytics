WITH sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source')}}

), sfdc_user_roles AS (

    SELECT *
    FROM {{ ref('sfdc_user_roles_source')}}

), final_users AS (

    SELECT
      sfdc_users.user_id                                            AS dim_crm_sales_rep_id,
      sfdc_users.name                                               AS rep_name,
      sfdc_users.title,
      sfdc_users.department,
      sfdc_users.team,
      sfdc_users.manager_id,
      sfdc_users.is_active,
      sfdc_users.start_date,
      sfdc_users.user_role_id,
      sfdc_user_roles.name                                          AS user_role_name,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_segment']) }}    AS dim_crm_user_sales_segment_id,
      sfdc_users.user_segment                                       AS crm_user_sales_segment,
      sfdc_users.user_segment_grouped                               AS crm_user_sales_segment_grouped,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_geo']) }}        AS dim_crm_user_geo_id,
      sfdc_users.user_geo                                           AS crm_user_geo,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_region']) }}     AS dim_crm_user_region_id,
      sfdc_users.user_region                                        AS crm_user_region,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_area']) }}       AS dim_crm_user_area_id,
      sfdc_users.user_area                                          AS crm_user_area,
      sfdc_users.user_segment_region_grouped                        AS crm_user_sales_segment_region_grouped
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles
      ON sfdc_users.user_role_id = sfdc_user_roles.id

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-12",
    updated_date="2021-03-26"
) }}
