{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_service_ping_instance', 'prep_service_ping_instance')
    ])

}}

, usage_data_w_date AS (
  SELECT
    prep_service_ping_instance.*,
    dim_date.date_id              AS dim_date_id
  FROM prep_service_ping_instance
  LEFT JOIN dim_date
    ON TO_DATE(ping_created_at) = dim_date.date_day

), last_ping_of_month_flag AS (

SELECT
    usage_data_w_date.id,
    usage_data_w_date.dim_date_id,
    usage_data_w_date.uuid,
    usage_data_w_date.host_id,
    usage_data_w_date.ping_created_at::TIMESTAMP(0)   AS ping_created_at,
    dim_date.month_actual,
    TRUE                      AS last_ping_of_month_flag
  FROM usage_data_w_date
    INNER JOIN dim_date
  ON usage_data_w_date.dim_date_id = dim_date.date_id
  QUALIFY ROW_NUMBER() OVER (
          PARTITION BY usage_data_w_date.uuid, usage_data_w_date.host_id, dim_date.month_actual
          ORDER BY ping_created_at DESC) = 1

), fct_w_month_flag AS (

  SELECT
      usage_data_w_date.*,
      last_ping_of_month_flag.last_ping_of_month_flag
    FROM usage_data_w_date
      LEFT JOIN last_ping_of_month_flag
        ON usage_data_w_date.id = last_ping_of_month_flag.id

), final AS (

    SELECT
      id                                                                        AS dim_service_ping_instance_id,
      dim_date_id                                                               AS dim_date_id,
      ping_created_at::TIMESTAMP(0)                                             AS ping_created_at,
      DATEADD('days', -28, ping_created_at)                                     AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                                       AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)                                      AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                                       AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                                        AS ping_created_at_date,
      ip_address_hash                                                           AS ip_address_hash,
      version,
      instance_user_count,
      license_md5,
      historical_max_users,
      license_user_count,
      license_starts_at,
      license_expires_at,
      license_add_ons,
      recorded_at,
      updated_at,
      mattermost_enabled,
      uuid                                                                      AS dim_instance_id,
      host_id || dim_instance_id                                                AS dim_installation_id,
      main_edition                                                              AS edition,
      hostname                                                                  AS host_name,
      host_id                                                                   AS dim_host_id,
      license_trial                                                             AS is_trial,
      source_license_id,
      installation_type,
      license_plan,
      database_adapter,
      database_version,
      git_version,
      gitlab_pages_enabled,
      gitlab_pages_version,
      container_registry_enabled,
      elasticsearch_enabled,
      geo_enabled,
      gitlab_shared_runners_enabled,
      gravatar_enabled,
      ldap_enabled,
      omniauth_enabled,
      reply_by_email_enabled,
      signup_enabled,
      prometheus_metrics_enabled,
      usage_activity_by_stage,
      usage_activity_by_stage_monthly,
      gitaly_clusters,
      gitaly_version,
      gitaly_servers,
      gitaly_filesystems,
      gitpod_enabled,
      object_store,
      is_dependency_proxy_enabled,
      recording_ce_finished_at,
      recording_ee_finished_at,
      stats_used,
      counts,
      is_ingress_modsecurity_enabled,
      topology,
      is_grafana_link_enabled,
      analytics_unique_visits,
      raw_usage_data_id,
      container_registry_vendor,
      container_registry_version,
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')                  AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                               AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                                       AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                   AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                   AS minor_version,
      major_version || '.' || minor_version                                                                         AS major_minor_version,
      major_version * 100 + minor_version                                                                           AS major_minor_version_id,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                                         AS service_ping_delivery_type,
      CASE
        WHEN service_ping_delivery_type = 'SaaS'                THEN TRUE
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
          WHEN last_ping_of_month_flag = TRUE      THEN TRUE
          ELSE FALSE
          END                                                                                                       AS is_last_ping_of_month
    FROM fct_w_month_flag

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@@icooper-acp",
    updated_by="@@icooper-acp",
    created_date="2022-03-08",
    updated_date="2022-03-11"
) }}
