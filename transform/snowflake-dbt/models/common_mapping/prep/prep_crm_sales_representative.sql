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
      {{ dbt_utils.surrogate_key(['sfdc_users.user_segment']) }}    AS dim_crm_sales_hierarchy_sales_segment_live_id,
      sfdc_users.user_segment                                       AS sales_segment_name_live,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_geo']) }}        AS dim_crm_sales_hierarchy_location_region_live_id,
      sfdc_users.user_geo                                           AS location_region_name_live,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_region']) }}     AS dim_crm_sales_hierarchy_sales_region_live_id,
      sfdc_users.user_region                                        AS sales_region_name_live,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_area']) }}       AS dim_crm_sales_hierarchy_sales_area_live_id,
      sfdc_users.user_area                                          AS sales_area_name_live,
      sfdc_users.sales_segment_name_live_grouped,
      sfdc_users.segment_region_live_grouped
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles
      ON sfdc_users.user_role_id = sfdc_user_roles.id

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2021-01-12",
    updated_date="2021-03-24"
) }}
