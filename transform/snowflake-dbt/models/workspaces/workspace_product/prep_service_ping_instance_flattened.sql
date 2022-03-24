{{ config(
    tags=["product", "mnpi_exception"],
    full_refresh = false,
    materialized = "incremental",
    unique_key = "prep_service_ping_instance_flattened_id"
) }}


WITH source AS (

    SELECT
        *
    FROM {{ ref('prep_service_ping_instance')}} as usage
      WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%') and ping_created_at >= '2022-01-01'
    {% if is_incremental() %}
                AND ping_created_at >= (SELECT COALESCE(MAX(ping_created_at), '2022-03-01') FROM {{this}})
    {% endif %}

) , flattened_high_level as (
      SELECT
        {{ dbt_utils.surrogate_key(['dim_service_ping_instance_id', 'path']) }}       AS prep_service_ping_instance_flattened_id,
        dim_service_ping_instance_id,
        dim_host_id,
        dim_instance_id,
        ping_created_at,
        ip_address_hash,
        id,
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
        uuid,
        hostname,
        host_id,
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
        raw_usage_data_payload,
        container_registry_vendor,
        container_registry_version,
        raw_usage_data_payload_reconstructed,
        main_edition,
        product_tier,
        path                                  AS metrics_path,
        value                                 AS metric_value
    FROM source,
      LATERAL FLATTEN(input => raw_usage_data_payload,
      RECURSIVE => true)
  )

  {{ dbt_audit(
      cte_ref="flattened_high_level",
      created_by="@icooper-acp",
      updated_by="@icooper-acp",
      created_date="2022-03-17",
      updated_date="2022-03-17"
  ) }}
