WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), sfdc_users_base AS (

    SELECT *
    FROM {{ ref('sfdc_user_snapshots_source')}}

), sfdc_users AS (

    SELECT
      {{ dbt_utils.surrogate_key(['sfdc_users_base.user_id','snapshot_dates.date_id'])}}    AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                AS snapshot_date_id,
      sfdc_users_base.*
    FROM sfdc_users_base
    INNER JOIN snapshot_dates
    ON snapshot_dates.date_actual >= sfdc_users_base.dbt_valid_from
      AND snapshot_dates.date_actual < COALESCE(sfdc_users_base.dbt_valid_to, '9999-12-31'::TIMESTAMP)

), sfdc_user_roles AS (

    SELECT *
    FROM {{ ref('sfdc_user_roles_source')}}

), final_users AS (

    SELECT
      sfdc_users.crm_user_snapshot_id,
      sfdc_users.snapshot_date_id,
      sfdc_users.user_id                                            AS dim_crm_user_id,
      sfdc_users.employee_number,
      sfdc_users.name                                               AS user_name,
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
      sfdc_users.user_segment_region_grouped                        AS crm_user_sales_segment_region_grouped,
      created_date
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles
      ON sfdc_users.user_role_id = sfdc_user_roles.id

)

{{ dbt_audit(
    cte_ref="final_users",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2022-01-20"
) }}
