{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_id"
) }}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('dim_product_tier', 'dim_product_tier'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('dim_crm_account','dim_crm_account')
    ])

}}

, map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash                 AS ip_address_hash,
      map_ip_to_country.dim_location_country_id         AS dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_ping_instance.*,
      prep_ping_instance.raw_usage_data_payload:license_billable_users::NUMBER                            AS license_billable_users, 
      TO_DATE(prep_ping_instance.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (prep_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      prep_ping_instance.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value
    FROM prep_ping_instance
      {% if is_incremental() %}
                  WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      map_ip_location.dim_location_country_id     AS dim_location_country_id
    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), prep_usage_ping_cte AS (

    SELECT
      dim_ping_instance_id                                AS dim_ping_instance_id,
      dim_host_id                                         AS dim_host_id,
      dim_instance_id                                     AS dim_instance_id,
      dim_installation_id                                 AS dim_installation_id,
      dim_product_tier.dim_product_tier_id                AS dim_product_tier_id,
      ping_created_at                                     AS ping_created_at,
      hostname                                            AS hostname,
      license_md5                                         AS license_md5,
      dim_location_country_id                             AS dim_location_country_id,
      license_trial_ends_on                               AS license_trial_ends_on,
      license_subscription_id                             AS license_subscription_id,
      license_billable_users                              AS license_billable_users,
      Instance_user_count                                 AS instance_user_count,
      historical_max_users                                AS historical_max_users,
      license_user_count                                  AS license_user_count,
      umau_value                                          AS umau_value,
      product_tier                                        AS product_tier,
      main_edition                                        AS main_edition
    FROM add_country_info_to_usage_ping
    LEFT JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND IFF( add_country_info_to_usage_ping.dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f','SaaS','Self-Managed') = dim_product_tier.product_delivery_type
    --AND main_edition = 'EE'

), joined_payload AS (

    SELECT
      {{ dbt_utils.surrogate_key(['prep_usage_ping_cte.dim_ping_instance_id']) }}                         AS ping_instance_id,
      prep_usage_ping_cte.dim_ping_instance_id                                                            AS dim_ping_instance_id,
      prep_usage_ping_cte.ping_created_at                                                                 AS ping_created_at,
      prep_usage_ping_cte.dim_product_tier_id                                                             AS dim_product_tier_id,
      COALESCE(prep_usage_ping_cte.license_subscription_id, prep_subscription.dim_subscription_id)        AS dim_subscription_id,
      prep_subscription.dim_crm_account_id                                                                AS dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id                                                           AS dim_parent_crm_account_id,
      prep_usage_ping_cte.dim_location_country_id                                                         AS dim_location_country_id,
      dim_date.date_id                                                                                    AS dim_ping_date_id,
      prep_usage_ping_cte.dim_instance_id                                                                 AS dim_instance_id,
      prep_usage_ping_cte.dim_host_id                                                                     AS dim_host_id,
      prep_usage_ping_cte.dim_installation_id                                                             AS dim_installation_id,
      prep_license.dim_license_id                                                                         AS dim_license_id,
      prep_usage_ping_cte.license_md5                                                                     AS license_md5,
      prep_usage_ping_cte.license_billable_users                                                          AS license_billable_users,
      prep_usage_ping_cte.Instance_user_count                                                             AS instance_user_count,
      prep_usage_ping_cte.historical_max_users                                                            AS historical_max_user_count,
      prep_usage_ping_cte.license_user_count                                                              AS license_user_count,
      prep_usage_ping_cte.hostname                                                                        AS hostname,
      prep_usage_ping_cte.umau_value                                                                      AS umau_value,
      prep_usage_ping_cte.license_subscription_id                                                         AS dim_subscription_license_id,
      IFF(prep_usage_ping_cte.ping_created_at < license_trial_ends_on, TRUE, FALSE)                       AS is_trial,
      prep_usage_ping_cte.product_tier                                                                    AS product_tier,
      prep_usage_ping_cte.main_edition                                                                    AS main_edition_product_tier,
      'VERSION_DB'                                                                                        AS data_source
    FROM prep_usage_ping_cte
    LEFT JOIN prep_license
      ON prep_usage_ping_cte.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_crm_account
      ON prep_subscription.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined_payload",
    created_by="@icooper-acp",
    updated_by="@snalamaru",
    created_date="2022-03-08",
    updated_date="2022-07-07"
) }}
