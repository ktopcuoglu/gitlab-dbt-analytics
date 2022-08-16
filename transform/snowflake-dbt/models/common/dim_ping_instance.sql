{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license','prep_license'),
    ('prep_charge','prep_charge'),
    ('prep_product_detail','prep_product_detail')
    ])

}}

, usage_data_w_date AS (
  SELECT
    prep_ping_instance.*,
    dim_date.date_id                                 AS dim_ping_date_id
  FROM prep_ping_instance
  LEFT JOIN dim_date
    ON TO_DATE(ping_created_at) = dim_date.date_day

), last_ping_of_month_flag AS (

SELECT DISTINCT
    usage_data_w_date.id                              AS id,
    usage_data_w_date.dim_ping_date_id                AS dim_ping_date_id,
    usage_data_w_date.uuid                            AS uuid,
    usage_data_w_date.host_id                         AS host_id,
    usage_data_w_date.ping_created_at::TIMESTAMP(0)   AS ping_created_at,
    dim_date.first_day_of_month                       AS first_day_of_month,
    TRUE                                              AS last_ping_of_month_flag,
    usage_data_w_date.raw_usage_data_payload          AS raw_usage_data_payload
  FROM usage_data_w_date
  INNER JOIN dim_date
    ON usage_data_w_date.dim_ping_date_id = dim_date.date_id
  QUALIFY ROW_NUMBER() OVER (
          PARTITION BY usage_data_w_date.uuid, usage_data_w_date.host_id, dim_date.first_day_of_month
          ORDER BY ping_created_at DESC) = 1

), last_ping_of_week_flag AS (

  SELECT DISTINCT
    usage_data_w_date.id                              AS id,
    usage_data_w_date.dim_ping_date_id                AS dim_ping_date_id,
    usage_data_w_date.uuid                            AS uuid,
    usage_data_w_date.host_id                         AS host_id,
    usage_data_w_date.ping_created_at::TIMESTAMP(0)   AS ping_created_at,
    dim_date.first_day_of_month                       AS first_day_of_month,
    TRUE                                              AS last_ping_of_week_flag
  FROM usage_data_w_date
  INNER JOIN dim_date
    ON usage_data_w_date.dim_ping_date_id = dim_date.date_id
  QUALIFY ROW_NUMBER() OVER (
          PARTITION BY usage_data_w_date.uuid, usage_data_w_date.host_id, dim_date.first_day_of_week
          ORDER BY ping_created_at DESC) = 1

), fct_w_month_flag AS (

  SELECT
    usage_data_w_date.*,
    last_ping_of_month_flag.last_ping_of_month_flag   AS last_ping_of_month_flag,
    last_ping_of_week_flag.last_ping_of_week_flag     AS last_ping_of_week_flag
  FROM usage_data_w_date
  LEFT JOIN last_ping_of_month_flag
    ON usage_data_w_date.id = last_ping_of_month_flag.id
  LEFT JOIN last_ping_of_week_flag
    ON usage_data_w_date.id = last_ping_of_week_flag.id

), dedicated_instance AS (

  SELECT DISTINCT prep_ping_instance.uuid
  FROM prep_ping_instance
  INNER JOIN prep_license
    ON prep_ping_instance.license_md5 = prep_license.license_md5
  INNER JOIN prep_charge
    ON prep_license.dim_subscription_id = prep_charge.dim_subscription_id
  INNER JOIN prep_product_detail
    ON prep_charge.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE LOWER(prep_product_detail.product_rate_plan_charge_name) LIKE '%dedicated%'

), final AS (

    SELECT DISTINCT
      dim_ping_instance_id                                                                                          AS dim_ping_instance_id,
      dim_ping_date_id                                                                                              AS dim_ping_date_id,
      dim_host_id                                                                                                   AS dim_host_id,
      dim_instance_id                                                                                               AS dim_instance_id,
      dim_installation_id                                                                                           AS dim_installation_id,
      ping_created_at                                                                                               AS ping_created_at,
      TO_DATE(DATEADD('days', -28, ping_created_at))                                                                AS ping_created_date_28_days_earlier,
      TO_DATE(DATE_TRUNC('YEAR', ping_created_at))                                                                  AS ping_created_date_year,
      TO_DATE(DATE_TRUNC('MONTH', ping_created_at))                                                                 AS ping_created_date_month,
      TO_DATE(DATE_TRUNC('WEEK', ping_created_at))                                                                  AS ping_created_date_week,
      TO_DATE(DATE_TRUNC('DAY', ping_created_at))                                                                   AS ping_created_date,
      ip_address_hash                                                                                               AS ip_address_hash,
      version                                                                                                       AS version,
      instance_user_count                                                                                           AS instance_user_count,
      license_md5                                                                                                   AS license_md5,
      historical_max_users                                                                                          AS historical_max_users,
      license_user_count                                                                                            AS license_user_count,
      license_starts_at                                                                                             AS license_starts_at,
      license_expires_at                                                                                            AS license_expires_at,
      license_add_ons                                                                                               AS license_add_ons,
      recorded_at                                                                                                   AS recorded_at,
      updated_at                                                                                                    AS updated_at,
      mattermost_enabled                                                                                            AS mattermost_enabled,
      main_edition                                                                                                  AS ping_edition,
      hostname                                                                                                      AS host_name,
      product_tier                                                                                                  AS product_tier,
      license_trial                                                                                                 AS is_trial,
      source_license_id                                                                                             AS source_license_id,
      installation_type                                                                                             AS installation_type,
      license_plan                                                                                                  AS license_plan,
      database_adapter                                                                                              AS database_adapter,
      database_version                                                                                              AS database_version,
      git_version                                                                                                   AS git_version,
      gitlab_pages_enabled                                                                                          AS gitlab_pages_enabled,
      gitlab_pages_version                                                                                          AS gitlab_pages_version,
      container_registry_enabled                                                                                    AS container_registry_enabled,
      elasticsearch_enabled                                                                                         AS elasticsearch_enabled,
      geo_enabled                                                                                                   AS geo_enabled,
      gitlab_shared_runners_enabled                                                                                 AS gitlab_shared_runners_enabled,
      gravatar_enabled                                                                                              AS gravatar_enabled,
      ldap_enabled                                                                                                  AS ldap_enabled,
      omniauth_enabled                                                                                              AS omniauth_enabled,
      reply_by_email_enabled                                                                                        AS reply_by_email_enabled,
      signup_enabled                                                                                                AS signup_enabled,
      prometheus_metrics_enabled                                                                                    AS prometheus_metrics_enabled,
      usage_activity_by_stage                                                                                       AS usage_activity_by_stage,
      usage_activity_by_stage_monthly                                                                               AS usage_activity_by_stage_monthly,
      gitaly_clusters                                                                                               AS gitaly_clusters,
      gitaly_version                                                                                                AS gitaly_version,
      gitaly_servers                                                                                                AS gitaly_servers,
      gitaly_filesystems                                                                                            AS gitaly_filesystems,
      gitpod_enabled                                                                                                AS gitpod_enabled,
      object_store                                                                                                  AS object_store,
      is_dependency_proxy_enabled                                                                                   AS is_dependency_proxy_enabled,
      recording_ce_finished_at                                                                                      AS recording_ce_finished_at,
      recording_ee_finished_at                                                                                      AS recording_ee_finished_at,
      is_ingress_modsecurity_enabled                                                                                AS is_ingress_modsecurity_enabled,
      topology                                                                                                      AS topology,
      is_grafana_link_enabled                                                                                       AS is_grafana_link_enabled,
      analytics_unique_visits                                                                                       AS analytics_unique_visits,
      raw_usage_data_id                                                                                             AS raw_usage_data_id,
      container_registry_vendor                                                                                     AS container_registry_vendor,
      container_registry_version                                                                                    AS container_registry_version,
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, ping_edition, 'EE Free')             AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                               AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                                       AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                   AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                   AS minor_version,
      major_version || '.' || minor_version                                                                         AS major_minor_version,
      major_version * 100 + minor_version                                                                           AS major_minor_version_id,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                                         AS ping_delivery_type,
      CASE
        WHEN EXISTS (SELECT 1 FROM dedicated_instance di
                     WHERE fct_w_month_flag.uuid = di.uuid)     THEN TRUE
        ELSE FALSE END                                                                                              AS is_saas_dedicated,
      CASE
        WHEN ping_delivery_type = 'SaaS'                        THEN TRUE
        WHEN installation_type = 'gitlab-development-kit'       THEN TRUE
        WHEN hostname = 'gitlab.com'                            THEN TRUE
        WHEN hostname ILIKE '%.gitlab.com'                      THEN TRUE
        ELSE FALSE END                                                                                              AS is_internal,
      CASE
        WHEN hostname ilike 'staging.%'                         THEN TRUE
        WHEN hostname IN (
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                                              AS is_staging,
      CASE
        WHEN last_ping_of_month_flag = TRUE                     THEN TRUE
        ELSE FALSE
      END                                                                                                           AS is_last_ping_of_month,
      CASE
        WHEN last_ping_of_week_flag = TRUE                     THEN TRUE
        ELSE FALSE
      END                                                                                                           AS is_last_ping_of_week,
      raw_usage_data_payload
    FROM fct_w_month_flag

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-03-08",
    updated_date="2022-08-08"
) }}
