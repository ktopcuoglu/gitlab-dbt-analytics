{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, final AS (

    SELECT top 1000
      id                                                                        AS dim_service_ping_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      DATEADD('days', -28, ping_created_at)                                     AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                                       AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)                                      AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                                       AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                                        AS ping_created_at_date,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash,
      source_ip,
      version,
      instance_user_count,
      license_md5,
      historical_max_users,
      license_user_count,
      license_starts_at,
      license_expires_at,
      license_add_ons,
      recordered_at,
      created_at,
      updated_at,
      mattermost_enabled,
      uuid                                                                      AS dim_instance_id,
      edition,
      hostname,
      host_id                                                                   AS dim_host_id,
      license_trial,
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
      gitaly_clusers,
      gitaly_versions,
      gitaly_servers,
      gitaly_filesystems,
      gitpod_enabled,
      object_store,
      is_dependency_proxy_enabled,
      recording_ce_finished_at,
      recording_ee_finished_at,
      stats_used,
      counts,
      in_ingress_modsecurity_enabled,
      topology,
      is_grafana_link_enabled,
      analytics_unique_visits,
      raw_usage_data_id,
      container_registry_vendor,
      container_registry_version,
      raw_usage_data_payload_reconstructed,
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')                  AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                               AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                                       AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                   AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                   AS minor_version,
      major_version || '.' || minor_version                                                                         AS major_minor_version,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                     AS ping_source,
      CASE
        WHEN ping_source = 'SaaS'                               THEN TRUE
        WHEN installation_type = 'gitlab-development-kit'       THEN TRUE
        WHEN hostname = 'gitlab.com'                            THEN TRUE
        WHEN hostname ILIKE '%.gitlab.com'                      THEN TRUE
        ELSE FALSE END                                                                          AS is_internal,
      CASE
        WHEN hostname ilike 'staging.%'                         THEN TRUE
        WHEN hostname IN (
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                          AS is_staging,
        hostname                                                                                AS host_name,
    FROM {{ ref('version_usage_data_source') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@@icooper-acp",
    updated_by="@@icooper-acp",
    created_date="2022-03-08",
    updated_date="2022-03-08"
) }}
