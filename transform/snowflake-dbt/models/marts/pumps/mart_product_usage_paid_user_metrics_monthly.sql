{{
  config(
    tags=["product", "mnpi_exception"],
    schema="common_mart_product"
  )
}}

{{ simple_cte([
    ('monthly_saas_metrics','fct_saas_product_usage_metrics_monthly'),
    ('monthly_sm_metrics','fct_product_usage_wave_1_3_metrics_monthly'),
    ('billing_accounts','dim_billing_account'),
    ('location_country', 'dim_location_country'),
    ('subscriptions', 'dim_subscription'),
    ('namespaces', 'dim_namespace'),
    ('charges', 'mart_charge'),
    ('dates', 'dim_date'),
    ('aggregated_metrics', 'redis_namespace_snowplow_clicks_aggregated_workspace')
]) }}


, most_recent_subscription_version AS (
    SELECT
      subscription_name,
      subscription_status,
      subscription_start_date,
      subscription_end_date,
      ROW_NUMBER() OVER(
        PARTITION BY
          subscription_name
        ORDER BY
          subscription_version DESC
      )
    FROM subscriptions
    WHERE subscription_status IN (
      'Active',
      'Cancelled'
    )
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY
        subscription_name
      ORDER BY
        subscription_version DESC
    ) = 1

), zuora_licenses_per_subscription AS (
  
    SELECT
      dates.first_day_of_month AS month,
      subscriptions.dim_subscription_id_original,
      SUM(charges.quantity) AS license_user_count
    FROM charges
    JOIN dates ON charges.effective_start_month <= dates.date_actual
      AND (charges.effective_end_month > dates.date_actual
       OR charges.effective_end_month IS NULL)
      AND dates.day_of_month = 1
    LEFT JOIN subscriptions ON charges.dim_subscription_id = subscriptions.dim_subscription_id
    WHERE charges.subscription_status IN ('Active','Cancelled')
      AND charges.product_tier_name != 'Storage'
    {{ dbt_utils.group_by(n = 2) }}
    
), action_active_users_project_repo_users AS (
  
    SELECT
      *
    FROM aggregated_metrics 
    WHERE event_action = 'action_active_users_project_repo'
  
), sm_paid_user_metrics AS (

    SELECT
      monthly_sm_metrics.snapshot_month,
      monthly_sm_metrics.dim_subscription_id,
      NULL                                                                         AS dim_namespace_id,
      NULL                                                                         AS namespace_name,
      monthly_sm_metrics.uuid,
      monthly_sm_metrics.hostname,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('billing_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      monthly_sm_metrics.dim_subscription_id_original,
      subscriptions.subscription_name,
      subscriptions.subscription_status,
      most_recent_subscription_version.subscription_status AS subscription_status_most_recent_version,
      subscriptions.term_start_date,
      subscriptions.term_end_date,
      most_recent_subscription_version.subscription_start_date,
      most_recent_subscription_version.subscription_end_date,
      monthly_sm_metrics.snapshot_date_id,
      monthly_sm_metrics.ping_created_at,
      monthly_sm_metrics.dim_usage_ping_id,
      monthly_sm_metrics.instance_type,
      monthly_sm_metrics.cleaned_version,
      location_country.country_name,
      location_country.iso_2_country_code,
      location_country.iso_3_country_code,
      'Self-Managed'                                                                AS delivery_type,
      -- Wave 1
      DIV0(
        monthly_sm_metrics.billable_user_count, 
        COALESCE(
          zuora_licenses_per_subscription.license_user_count, 
          monthly_sm_metrics.license_user_count)
      )                                                                             AS license_utilization,
      monthly_sm_metrics.billable_user_count,
      monthly_sm_metrics.active_user_count,
      monthly_sm_metrics.max_historical_user_count,
      COALESCE(
        zuora_licenses_per_subscription.license_user_count, 
        monthly_sm_metrics.license_user_count)                                      AS license_user_count,
      IFF(
        zuora_licenses_per_subscription.license_user_count IS NOT NULL, 
        'Zuora',
        'Service Ping')                                                             AS license_user_count_source,
      -- Wave 2 & 3
      monthly_sm_metrics.umau_28_days_user,
      monthly_sm_metrics.action_monthly_active_users_project_repo_28_days_user,
      monthly_sm_metrics.merge_requests_28_days_user,
      monthly_sm_metrics.projects_with_repositories_enabled_28_days_user,
      monthly_sm_metrics.commit_comment_all_time_event,
      monthly_sm_metrics.source_code_pushes_all_time_event,
      monthly_sm_metrics.ci_pipelines_28_days_user,
      monthly_sm_metrics.ci_internal_pipelines_28_days_user,
      monthly_sm_metrics.ci_builds_28_days_user,
      monthly_sm_metrics.ci_builds_all_time_user,
      monthly_sm_metrics.ci_builds_all_time_event,
      monthly_sm_metrics.ci_runners_all_time_event,
      monthly_sm_metrics.auto_devops_enabled_all_time_event,
      monthly_sm_metrics.gitlab_shared_runners_enabled,
      monthly_sm_metrics.container_registry_enabled,
      monthly_sm_metrics.template_repositories_all_time_event,
      monthly_sm_metrics.ci_pipeline_config_repository_28_days_user,
      monthly_sm_metrics.user_unique_users_all_secure_scanners_28_days_user,
      monthly_sm_metrics.user_sast_jobs_28_days_user,
      monthly_sm_metrics.user_dast_jobs_28_days_user,
      monthly_sm_metrics.user_dependency_scanning_jobs_28_days_user,
      monthly_sm_metrics.user_license_management_jobs_28_days_user,
      monthly_sm_metrics.user_secret_detection_jobs_28_days_user,
      monthly_sm_metrics.user_container_scanning_jobs_28_days_user,
      monthly_sm_metrics.object_store_packages_enabled,
      monthly_sm_metrics.projects_with_packages_all_time_event,
      monthly_sm_metrics.projects_with_packages_28_days_user,
      monthly_sm_metrics.deployments_28_days_user,
      monthly_sm_metrics.releases_28_days_user,
      monthly_sm_metrics.epics_28_days_user,
      monthly_sm_metrics.issues_28_days_user,
      -- Wave 3.1
      monthly_sm_metrics.ci_internal_pipelines_all_time_event,
      monthly_sm_metrics.ci_external_pipelines_all_time_event,
      monthly_sm_metrics.merge_requests_all_time_event,
      monthly_sm_metrics.todos_all_time_event,
      monthly_sm_metrics.epics_all_time_event,
      monthly_sm_metrics.issues_all_time_event,
      monthly_sm_metrics.projects_all_time_event,
      monthly_sm_metrics.deployments_28_days_event,
      monthly_sm_metrics.packages_28_days_event,
      monthly_sm_metrics.sast_jobs_all_time_event,
      monthly_sm_metrics.dast_jobs_all_time_event,
      monthly_sm_metrics.dependency_scanning_jobs_all_time_event,
      monthly_sm_metrics.license_management_jobs_all_time_event,
      monthly_sm_metrics.secret_detection_jobs_all_time_event,
      monthly_sm_metrics.container_scanning_jobs_all_time_event,
      monthly_sm_metrics.projects_jenkins_active_all_time_event,
      monthly_sm_metrics.projects_bamboo_active_all_time_event,
      monthly_sm_metrics.projects_jira_active_all_time_event,
      monthly_sm_metrics.projects_drone_ci_active_all_time_event,
      monthly_sm_metrics.projects_github_active_all_time_event,
      monthly_sm_metrics.projects_jira_server_active_all_time_event,
      monthly_sm_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      monthly_sm_metrics.projects_with_repositories_enabled_all_time_event,
      monthly_sm_metrics.protected_branches_all_time_event,
      monthly_sm_metrics.remote_mirrors_all_time_event,
      monthly_sm_metrics.projects_enforcing_code_owner_approval_28_days_user,
      monthly_sm_metrics.project_clusters_enabled_28_days_user,
      monthly_sm_metrics.analytics_28_days_user,
      monthly_sm_metrics.issues_edit_28_days_user,
      monthly_sm_metrics.user_packages_28_days_user,
      monthly_sm_metrics.terraform_state_api_28_days_user,
      monthly_sm_metrics.incident_management_28_days_user,
      -- Wave 3.2
      monthly_sm_metrics.auto_devops_enabled,
      monthly_sm_metrics.gitaly_clusters_instance,
      monthly_sm_metrics.epics_deepest_relationship_level_instance,
      monthly_sm_metrics.clusters_applications_cilium_all_time_event,
      monthly_sm_metrics.network_policy_forwards_all_time_event,
      monthly_sm_metrics.network_policy_drops_all_time_event,
      monthly_sm_metrics.requirements_with_test_report_all_time_event,
      monthly_sm_metrics.requirement_test_reports_ci_all_time_event,
      monthly_sm_metrics.projects_imported_from_github_all_time_event,
      monthly_sm_metrics.projects_jira_cloud_active_all_time_event,
      monthly_sm_metrics.projects_jira_dvcs_server_active_all_time_event,
      monthly_sm_metrics.service_desk_issues_all_time_event,
      monthly_sm_metrics.ci_pipelines_all_time_user,
      monthly_sm_metrics.service_desk_issues_28_days_user,
      monthly_sm_metrics.projects_jira_active_28_days_user,
      monthly_sm_metrics.projects_jira_dvcs_cloud_active_28_days_user,
      monthly_sm_metrics.projects_jira_dvcs_server_active_28_days_user,
      monthly_sm_metrics.merge_requests_with_required_code_owners_28_days_user,
      monthly_sm_metrics.analytics_value_stream_28_days_event,
      monthly_sm_metrics.code_review_user_approve_mr_28_days_user,
      monthly_sm_metrics.epics_usage_28_days_user,
      monthly_sm_metrics.ci_templates_usage_28_days_event,
      monthly_sm_metrics.project_management_issue_milestone_changed_28_days_user,
      monthly_sm_metrics.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      monthly_sm_metrics.protected_branches_28_days_user,
      monthly_sm_metrics.ci_cd_lead_time_usage_28_days_event,
      monthly_sm_metrics.ci_cd_deployment_frequency_usage_28_days_event,
      monthly_sm_metrics.projects_with_repositories_enabled_all_time_user,
      monthly_sm_metrics.api_fuzzing_jobs_usage_28_days_user,
      monthly_sm_metrics.coverage_fuzzing_pipeline_usage_28_days_event,
      monthly_sm_metrics.api_fuzzing_pipeline_usage_28_days_event,
      monthly_sm_metrics.container_scanning_pipeline_usage_28_days_event,
      monthly_sm_metrics.dependency_scanning_pipeline_usage_28_days_event,
      monthly_sm_metrics.sast_pipeline_usage_28_days_event,
      monthly_sm_metrics.secret_detection_pipeline_usage_28_days_event,
      monthly_sm_metrics.dast_pipeline_usage_28_days_event,
      monthly_sm_metrics.coverage_fuzzing_jobs_28_days_user,
      monthly_sm_metrics.environments_all_time_event,
      monthly_sm_metrics.feature_flags_all_time_event,
      monthly_sm_metrics.successful_deployments_28_days_event,
      monthly_sm_metrics.failed_deployments_28_days_event,
      monthly_sm_metrics.projects_compliance_framework_all_time_event,
      monthly_sm_metrics.commit_ci_config_file_28_days_user,
      monthly_sm_metrics.view_audit_all_time_user,
      -- Wave 5.2
      monthly_sm_metrics.dependency_scanning_jobs_all_time_user,
      monthly_sm_metrics.analytics_devops_adoption_all_time_user,
      monthly_sm_metrics.projects_imported_all_time_event,
      monthly_sm_metrics.preferences_security_dashboard_28_days_user,
      monthly_sm_metrics.web_ide_edit_28_days_user,
      monthly_sm_metrics.auto_devops_pipelines_all_time_event,
      monthly_sm_metrics.projects_prometheus_active_all_time_event,
      monthly_sm_metrics.prometheus_enabled,
      monthly_sm_metrics.prometheus_metrics_enabled,
      monthly_sm_metrics.group_saml_enabled,
      monthly_sm_metrics.jira_issue_imports_all_time_event,
      monthly_sm_metrics.author_epic_all_time_user,
      monthly_sm_metrics.author_issue_all_time_user,
      monthly_sm_metrics.failed_deployments_28_days_user,
      monthly_sm_metrics.successful_deployments_28_days_user,
      -- Wave 5.3
      monthly_sm_metrics.geo_enabled,
      monthly_sm_metrics.geo_nodes_all_time_event,
      monthly_sm_metrics.auto_devops_pipelines_28_days_user,
      monthly_sm_metrics.active_instance_runners_all_time_event,
      monthly_sm_metrics.active_group_runners_all_time_event,
      monthly_sm_metrics.active_project_runners_all_time_event,
      monthly_sm_metrics.gitaly_version,
      monthly_sm_metrics.gitaly_servers_all_time_event,
      -- Wave 6
      monthly_sm_metrics.api_fuzzing_scans_all_time_event,
      monthly_sm_metrics.api_fuzzing_scans_28_days_event,
      monthly_sm_metrics.coverage_fuzzing_scans_all_time_event,
      monthly_sm_metrics.coverage_fuzzing_scans_28_days_event,
      monthly_sm_metrics.secret_detection_scans_all_time_event,
      monthly_sm_metrics.secret_detection_scans_28_days_event,
      monthly_sm_metrics.dependency_scanning_scans_all_time_event,
      monthly_sm_metrics.dependency_scanning_scans_28_days_event,
      monthly_sm_metrics.container_scanning_scans_all_time_event,
      monthly_sm_metrics.container_scanning_scans_28_days_event,
      monthly_sm_metrics.dast_scans_all_time_event,
      monthly_sm_metrics.dast_scans_28_days_event,
      monthly_sm_metrics.sast_scans_all_time_event,
      monthly_sm_metrics.sast_scans_28_days_event,
      -- Data Quality Flag
      monthly_sm_metrics.is_latest_data
    FROM monthly_sm_metrics
    LEFT JOIN billing_accounts
      ON monthly_sm_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN location_country
      ON monthly_sm_metrics.dim_location_country_id = location_country.dim_location_country_id
    LEFT JOIN subscriptions
      ON subscriptions.dim_subscription_id = monthly_sm_metrics.dim_subscription_id
    LEFT JOIN most_recent_subscription_version
      ON subscriptions.subscription_name = most_recent_subscription_version.subscription_name
    LEFT JOIN zuora_licenses_per_subscription 
      ON zuora_licenses_per_subscription.dim_subscription_id_original = monthly_sm_metrics.dim_subscription_id_original
      AND zuora_licenses_per_subscription.month = monthly_sm_metrics.snapshot_month

), saas_paid_user_metrics AS (

    SELECT
      monthly_saas_metrics.snapshot_month,
      monthly_saas_metrics.dim_subscription_id,
      monthly_saas_metrics.dim_namespace_id::VARCHAR                                AS dim_namespace_id,
      namespaces.namespace_name,
      NULL                                                                          AS uuid,
      NULL                                                                          AS hostname,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('billing_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      monthly_saas_metrics.dim_subscription_id_original,
      subscriptions.subscription_name,
      subscriptions.subscription_status,
      most_recent_subscription_version.subscription_status AS subscription_status_most_recent_version,
      subscriptions.term_start_date,
      subscriptions.term_end_date,
      most_recent_subscription_version.subscription_start_date,
      most_recent_subscription_version.subscription_end_date,
      monthly_saas_metrics.snapshot_date_id,
      monthly_saas_metrics.ping_created_at,
      NULL                                                                          AS dim_usage_ping_id,
      monthly_saas_metrics.instance_type                                            AS instance_type,
      NULL                                                                          AS cleaned_version,
      NULL                                                                          AS country_name,
      NULL                                                                          AS iso_2_country_code,
      NULL                                                                          AS iso_3_country_code,
      'SaaS'                                                                        AS delivery_type,
      -- Wave 1
      DIV0(
        monthly_saas_metrics.billable_user_count, 
        COALESCE(
          zuora_licenses_per_subscription.license_user_count,
          monthly_saas_metrics.subscription_seats)
      )                                                                             AS license_utilization,
      monthly_saas_metrics.billable_user_count,
      NULL                                                                          AS active_user_count,
      monthly_saas_metrics.max_historical_user_count,
      COALESCE(
        zuora_licenses_per_subscription.license_user_count,
        monthly_saas_metrics.subscription_seats)                                    AS license_user_count,
      IFF(
        zuora_licenses_per_subscription.license_user_count IS NOT NULL,
        'Zuora',
        'gitlabdotcom')                                                             AS license_user_count_source,
      -- Wave 2 & 3
      monthly_saas_metrics.umau_28_days_user,
      COALESCE(action_active_users_project_repo_users.distinct_users, 0)            AS action_monthly_active_users_project_repo_28_days_user,
      monthly_saas_metrics.merge_requests_28_days_user,
      monthly_saas_metrics.projects_with_repositories_enabled_28_days_user,
      monthly_saas_metrics.commit_comment_all_time_event,
      monthly_saas_metrics.source_code_pushes_all_time_event,
      monthly_saas_metrics.ci_pipelines_28_days_user,
      monthly_saas_metrics.ci_internal_pipelines_28_days_user,
      monthly_saas_metrics.ci_builds_28_days_user,
      monthly_saas_metrics.ci_builds_all_time_user,
      monthly_saas_metrics.ci_builds_all_time_event,
      monthly_saas_metrics.ci_runners_all_time_event,
      monthly_saas_metrics.auto_devops_enabled_all_time_event,
      monthly_saas_metrics.gitlab_shared_runners_enabled,
      monthly_saas_metrics.container_registry_enabled,
      monthly_saas_metrics.template_repositories_all_time_event,
      monthly_saas_metrics.ci_pipeline_config_repository_28_days_user,
      monthly_saas_metrics.user_unique_users_all_secure_scanners_28_days_user,
      monthly_saas_metrics.user_sast_jobs_28_days_user,
      monthly_saas_metrics.user_dast_jobs_28_days_user,
      monthly_saas_metrics.user_dependency_scanning_jobs_28_days_user,
      monthly_saas_metrics.user_license_management_jobs_28_days_user,
      monthly_saas_metrics.user_secret_detection_jobs_28_days_user,
      monthly_saas_metrics.user_container_scanning_jobs_28_days_user,
      monthly_saas_metrics.object_store_packages_enabled,
      monthly_saas_metrics.projects_with_packages_all_time_event,
      monthly_saas_metrics.projects_with_packages_28_days_user,
      monthly_saas_metrics.deployments_28_days_user,
      monthly_saas_metrics.releases_28_days_user,
      monthly_saas_metrics.epics_28_days_user,
      monthly_saas_metrics.issues_28_days_user,
      -- Wave 3.1
      monthly_saas_metrics.ci_internal_pipelines_all_time_event,
      monthly_saas_metrics.ci_external_pipelines_all_time_event,
      monthly_saas_metrics.merge_requests_all_time_event,
      monthly_saas_metrics.todos_all_time_event,
      monthly_saas_metrics.epics_all_time_event,
      monthly_saas_metrics.issues_all_time_event,
      monthly_saas_metrics.projects_all_time_event,
      monthly_saas_metrics.deployments_28_days_event,
      monthly_saas_metrics.packages_28_days_event,
      monthly_saas_metrics.sast_jobs_all_time_event,
      monthly_saas_metrics.dast_jobs_all_time_event,
      monthly_saas_metrics.dependency_scanning_jobs_all_time_event,
      monthly_saas_metrics.license_management_jobs_all_time_event,
      monthly_saas_metrics.secret_detection_jobs_all_time_event,
      monthly_saas_metrics.container_scanning_jobs_all_time_event,
      monthly_saas_metrics.projects_jenkins_active_all_time_event,
      monthly_saas_metrics.projects_bamboo_active_all_time_event,
      monthly_saas_metrics.projects_jira_active_all_time_event,
      monthly_saas_metrics.projects_drone_ci_active_all_time_event,
      monthly_saas_metrics.projects_github_active_all_time_event,
      monthly_saas_metrics.projects_jira_server_active_all_time_event,
      monthly_saas_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      monthly_saas_metrics.projects_with_repositories_enabled_all_time_event,
      monthly_saas_metrics.protected_branches_all_time_event,
      monthly_saas_metrics.remote_mirrors_all_time_event,
      monthly_saas_metrics.projects_enforcing_code_owner_approval_28_days_user,
      monthly_saas_metrics.project_clusters_enabled_28_days_user,
      monthly_saas_metrics.analytics_28_days_user,
      monthly_saas_metrics.issues_edit_28_days_user,
      monthly_saas_metrics.user_packages_28_days_user,
      monthly_saas_metrics.terraform_state_api_28_days_user,
      monthly_saas_metrics.incident_management_28_days_user,
      -- Wave 3.2
      monthly_saas_metrics.auto_devops_enabled,
      monthly_saas_metrics.gitaly_clusters_instance,
      monthly_saas_metrics.epics_deepest_relationship_level_instance,
      monthly_saas_metrics.clusters_applications_cilium_all_time_event,
      monthly_saas_metrics.network_policy_forwards_all_time_event,
      monthly_saas_metrics.network_policy_drops_all_time_event,
      monthly_saas_metrics.requirements_with_test_report_all_time_event,
      monthly_saas_metrics.requirement_test_reports_ci_all_time_event,
      monthly_saas_metrics.projects_imported_from_github_all_time_event,
      monthly_saas_metrics.projects_jira_cloud_active_all_time_event,
      monthly_saas_metrics.projects_jira_dvcs_server_active_all_time_event,
      monthly_saas_metrics.service_desk_issues_all_time_event,
      monthly_saas_metrics.ci_pipelines_all_time_user,
      monthly_saas_metrics.service_desk_issues_28_days_user,
      monthly_saas_metrics.projects_jira_active_28_days_user,
      monthly_saas_metrics.projects_jira_dvcs_cloud_active_28_days_user,
      monthly_saas_metrics.projects_jira_dvcs_server_active_28_days_user,
      monthly_saas_metrics.merge_requests_with_required_code_owners_28_days_user,
      monthly_saas_metrics.analytics_value_stream_28_days_event,
      monthly_saas_metrics.code_review_user_approve_mr_28_days_user,
      monthly_saas_metrics.epics_usage_28_days_user,
      monthly_saas_metrics.ci_templates_usage_28_days_event,
      monthly_saas_metrics.project_management_issue_milestone_changed_28_days_user,
      monthly_saas_metrics.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      monthly_saas_metrics.protected_branches_28_days_user,
      monthly_saas_metrics.ci_cd_lead_time_usage_28_days_event,
      monthly_saas_metrics.ci_cd_deployment_frequency_usage_28_days_event,
      monthly_saas_metrics.projects_with_repositories_enabled_all_time_user,
      monthly_saas_metrics.api_fuzzing_jobs_usage_28_days_user,
      monthly_saas_metrics.coverage_fuzzing_pipeline_usage_28_days_event,
      monthly_saas_metrics.api_fuzzing_pipeline_usage_28_days_event,
      monthly_saas_metrics.container_scanning_pipeline_usage_28_days_event,
      monthly_saas_metrics.dependency_scanning_pipeline_usage_28_days_event,
      monthly_saas_metrics.sast_pipeline_usage_28_days_event,
      monthly_saas_metrics.secret_detection_pipeline_usage_28_days_event,
      monthly_saas_metrics.dast_pipeline_usage_28_days_event,
      monthly_saas_metrics.coverage_fuzzing_jobs_28_days_user,
      monthly_saas_metrics.environments_all_time_event,
      monthly_saas_metrics.feature_flags_all_time_event,
      monthly_saas_metrics.successful_deployments_28_days_event,
      monthly_saas_metrics.failed_deployments_28_days_event,
      monthly_saas_metrics.projects_compliance_framework_all_time_event,
      monthly_saas_metrics.commit_ci_config_file_28_days_user,
      monthly_saas_metrics.view_audit_all_time_user,
      -- Wave 5.2
      monthly_saas_metrics.dependency_scanning_jobs_all_time_user,
      monthly_saas_metrics.analytics_devops_adoption_all_time_user,
      monthly_saas_metrics.projects_imported_all_time_event,
      monthly_saas_metrics.preferences_security_dashboard_28_days_user,
      monthly_saas_metrics.web_ide_edit_28_days_user,
      monthly_saas_metrics.auto_devops_pipelines_all_time_event,
      monthly_saas_metrics.projects_prometheus_active_all_time_event,
      monthly_saas_metrics.prometheus_enabled,
      monthly_saas_metrics.prometheus_metrics_enabled,
      monthly_saas_metrics.group_saml_enabled,
      monthly_saas_metrics.jira_issue_imports_all_time_event,
      monthly_saas_metrics.author_epic_all_time_user,
      monthly_saas_metrics.author_issue_all_time_user,
      monthly_saas_metrics.failed_deployments_28_days_user,
      monthly_saas_metrics.successful_deployments_28_days_user,
      -- Wave 5.3
      monthly_saas_metrics.geo_enabled,
      monthly_saas_metrics.geo_nodes_all_time_event,
      monthly_saas_metrics.auto_devops_pipelines_28_days_user,
      monthly_saas_metrics.active_instance_runners_all_time_event,
      monthly_saas_metrics.active_group_runners_all_time_event,
      monthly_saas_metrics.active_project_runners_all_time_event,
      monthly_saas_metrics.gitaly_version,
      monthly_saas_metrics.gitaly_servers_all_time_event,
      -- Wave 6
      monthly_saas_metrics.api_fuzzing_scans_all_time_event,
      monthly_saas_metrics.api_fuzzing_scans_28_days_event,
      monthly_saas_metrics.coverage_fuzzing_scans_all_time_event,
      monthly_saas_metrics.coverage_fuzzing_scans_28_days_event,
      monthly_saas_metrics.secret_detection_scans_all_time_event,
      monthly_saas_metrics.secret_detection_scans_28_days_event,
      monthly_saas_metrics.dependency_scanning_scans_all_time_event,
      monthly_saas_metrics.dependency_scanning_scans_28_days_event,
      monthly_saas_metrics.container_scanning_scans_all_time_event,
      monthly_saas_metrics.container_scanning_scans_28_days_event,
      monthly_saas_metrics.dast_scans_all_time_event,
      monthly_saas_metrics.dast_scans_28_days_event,
      monthly_saas_metrics.sast_scans_all_time_event,
      monthly_saas_metrics.sast_scans_28_days_event,
      -- Data Quality Flag
      monthly_saas_metrics.is_latest_data
    FROM monthly_saas_metrics
    LEFT JOIN billing_accounts
      ON monthly_saas_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN subscriptions
      ON subscriptions.dim_subscription_id = monthly_saas_metrics.dim_subscription_id
    LEFT JOIN most_recent_subscription_version
      ON subscriptions.subscription_name = most_recent_subscription_version.subscription_name
    LEFT JOIN zuora_licenses_per_subscription 
      ON zuora_licenses_per_subscription.dim_subscription_id_original = monthly_saas_metrics.dim_subscription_id_original
      AND zuora_licenses_per_subscription.month = monthly_saas_metrics.snapshot_month
    LEFT JOIN namespaces 
      ON namespaces.dim_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN action_active_users_project_repo_users
      ON action_active_users_project_repo_users.date_month = monthly_saas_metrics.snapshot_month 
      AND action_active_users_project_repo_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id

), unioned AS (

    SELECT *
    FROM sm_paid_user_metrics

    UNION ALL

    SELECT *
    FROM saas_paid_user_metrics

), final AS (
  
    SELECT
      unioned.*,
      {{ dbt_utils.surrogate_key(
        [
          'snapshot_month',
          'dim_subscription_id',
          'delivery_type',
          'uuid',
          'hostname',
          'dim_namespace_id'
        ]
      ) }} AS primary_key
    FROM unioned
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@snalamaru",
    created_date="2021-06-11",
    updated_date="2022-06-28"
) }}
