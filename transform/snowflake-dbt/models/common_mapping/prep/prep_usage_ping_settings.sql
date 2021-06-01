{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_usage_data_flattened', 'prep_usage_data_flattened'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting')
    ])
    
}}

, usage_ping AS (

    SELECT 
      id                                                                        AS dim_usage_ping_id, 
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *, 
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash, 
      OBJECT_CONSTRUCT(
        {% for column in columns %}  
          '{{ column.name | lower }}', {{ column.name | lower }}
          {% if not loop.last %}
            ,
          {% endif %}
        {% endfor %}
      )                                                                         AS raw_usage_data_payload_reconstructed
    FROM {{ ref('version_usage_data_source') }}

), settings_data AS (

    SELECT
      ping_id AS dim_usage_ping_id,
      {% for column in settings_columns %}
        MAX(IFF(prep_usage_data_flattened.metrics_path = '{{column}}', metric_value, NULL)) AS {{column | replace(".","_")}}
        {{ "," if not loop.last }}
      {% endfor %}    
    FROM prep_usage_data_flattened
    INNER JOIN prep_usage_ping_metrics_setting ON prep_usage_data_flattened.metrics_path = prep_usage_ping_metrics_setting.metrics_path
    GROUP BY 1

), renamed AS (

    SELECT
      dim_usage_ping_id,
      container_registry_server_version                              AS container_registry_server_version,
      database_adapter                                               AS database_adapter,
      database_version                                               AS database_version,
      git_version                                                    AS git_version,
      gitaly_version                                                 AS gitaly_version,
      gitlab_pages_version                                           AS gitlab_pages_version,
      container_registry_enabled                                     AS is_container_registry_enabled,
      dependency_proxy_enabled                                       AS is_dependency_proxy_enabled,
      elasticsearch_enabled                                          AS is_elasticsearch_enabled,
      geo_enabled                                                    AS is_geo_enabled,
      gitlab_pages_enabled                                           AS is_gitlab_pages_enabled,
      gitpod_enabled                                                 AS is_gitpod_enabled,
      grafana_link_enabled                                           AS is_grafana_link_enabled,
      gravatar_enabled                                               AS is_gravatar_enabled,
      ingress_modsecurity_enabled                                    AS is_ingress_modsecurity_enabled,
      instance_auto_devops_enabled                                   AS is_instance_auto_devops_enabled,
      ldap_enabled                                                   AS is_ldap_enabled,
      mattermost_enabled                                             AS is_mattermost_enabled,
      object_store_artifacts_enabled                                 AS is_object_store_artifacts_enabled,
      object_store_artifacts_object_store_enabled                    AS is_object_store_artifacts_object_store_enabled,
      object_store_external_diffs_enabled                            AS is_object_store_external_diffs_enabled,
      object_store_external_diffs_object_store_enabled               AS is_object_store_external_diffs_object_store_enabled,
      object_store_lfs_enabled                                       AS is_object_store_lfs_enabled,
      object_store_packages_enabled                                  AS is_object_store_packages_enabled,
      object_store_packages_object_store_enabled                     AS is_object_store_packages_object_store_enabled,
      object_store_uploads_object_store_enabled                      AS is_object_store_uploads_object_store_enabled,
      omniauth_enabled                                               AS is_omniauth_enabled,
      prometheus_enabled                                             AS is_prometheus_enabled,
      prometheus_metrics_enabled                                     AS is_prometheus_metrics_enabled,
      reply_by_email_enabled                                         AS is_reply_by_email_enabled,
      settings_ldap_encrypted_secrets_enabled                        AS is_settings_ldap_encrypted_secrets_enabled,
      signup_enabled                                                 AS is_signup_enabled,
      usage_activity_by_stage_manage_group_saml_enabled              AS is_usage_activity_by_stage_manage_group_saml_enabled,
      usage_activity_by_stage_manage_ldap_admin_sync_enabled         AS is_usage_activity_by_stage_manage_ldap_admin_sync_enabled,
      usage_activity_by_stage_manage_ldap_group_sync_enabled         AS is_usage_activity_by_stage_manage_ldap_group_sync_enabled,
      usage_activity_by_stage_monthly_manage_group_saml_enabled      AS is_usage_activity_by_stage_monthly_manage_group_saml_enabled,
      usage_activity_by_stage_monthly_manage_ldap_admin_sync_enabled AS is_usage_activity_by_stage_monthly_manage_ldap_admin_sync_enabled,
      usage_activity_by_stage_monthly_manage_ldap_group_sync_enabled AS is_usage_activity_by_stage_monthly_manage_ldap_group_sync_enabled,
      web_ide_clientside_preview_enabled                             AS is_web_ide_clientside_preview_enabled,
      object_store_artifacts_object_store_background_upload          AS object_store_artifacts_object_store_background_upload,
      object_store_artifacts_object_store_direct_upload              AS object_store_artifacts_object_store_direct_upload,
      object_store_external_diffs_object_store_background_upload     AS object_store_external_diffs_object_store_background_upload,
      object_store_external_diffs_object_store_direct_upload         AS object_store_external_diffs_object_store_direct_upload,
      object_store_lfs_object_store_background_upload                AS object_store_lfs_object_store_background_upload,
      object_store_lfs_object_store_direct_upload                    AS object_store_lfs_object_store_direct_upload,
      object_store_packages_object_store_background_upload           AS object_store_packages_object_store_background_upload,
      object_store_packages_object_store_direct_upload               AS object_store_packages_object_store_direct_upload,
      object_store_uploads_object_store_background_upload            AS object_store_uploads_object_store_background_upload,
      object_store_uploads_object_store_direct_upload                AS object_store_uploads_object_store_direct_upload,
      settings_gitaly_apdex                                          AS settings_gitaly_apdex,
      settings_operating_system                                      AS settings_operating_system,
      topology                                                       AS topology
    FROM settings_data

)

SELECT *
FROM renamed
