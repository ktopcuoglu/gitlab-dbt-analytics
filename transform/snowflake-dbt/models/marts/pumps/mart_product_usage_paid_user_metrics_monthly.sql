{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('monthly_saas_metrics','fct_saas_product_usage_metrics_monthly'),
    ('monthly_sm_metrics','fct_product_usage_wave_1_3_metrics_monthly'),
    ('billing_accounts','dim_billing_account'),
    ('crm_accounts','dim_crm_account'),
    ('location_country', 'dim_location_country'),
    ('subscriptions', 'dim_subscription_snapshot_bottom_up')
]) }}

, sm_paid_user_metrics AS (

    SELECT
      monthly_sm_metrics.snapshot_month,
      monthly_sm_metrics.dim_subscription_id,
      NULL                                                                          AS dim_namespace_id,
      monthly_sm_metrics.uuid,
      monthly_sm_metrics.hostname,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      monthly_sm_metrics.dim_subscription_id_original,
      subscriptions.subscription_status,
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
      monthly_sm_metrics.license_utilization,
      monthly_sm_metrics.billable_user_count,
      monthly_sm_metrics.active_user_count,
      monthly_sm_metrics.max_historical_user_count,
      monthly_sm_metrics.license_user_count,
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
      monthly_sm_metrics.jira_imports_28_days_event,
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
      monthly_sm_metrics.is_latest_data
    FROM monthly_sm_metrics
    LEFT JOIN billing_accounts
      ON monthly_sm_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN crm_accounts
      ON billing_accounts.dim_crm_account_id = crm_accounts.dim_crm_account_id
    LEFT JOIN location_country
      ON monthly_sm_metrics.dim_location_country_id = location_country.dim_location_country_id
    LEFT JOIN subscriptions
      ON monthly_sm_metrics.dim_subscription_id = subscriptions.dim_subscription_id
      AND IFNULL(monthly_sm_metrics.ping_created_at::DATE, DATEADD('day', -1, monthly_sm_metrics.snapshot_month))
      = TO_DATE(TO_CHAR(subscriptions.snapshot_id), 'YYYYMMDD')

), saas_paid_user_metrics AS (

    SELECT
      monthly_saas_metrics.snapshot_month,
      monthly_saas_metrics.dim_subscription_id,
      monthly_saas_metrics.dim_namespace_id::VARCHAR                                AS dim_namespace_id,
      NULL                                                                          AS uuid,
      NULL                                                                          AS hostname,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      monthly_saas_metrics.dim_subscription_id_original,
      subscriptions.subscription_status,
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
      monthly_saas_metrics.license_utilization,
      monthly_saas_metrics.billable_user_count,
      NULL                                                                          AS active_user_count,
      monthly_saas_metrics.max_historical_user_count,
      monthly_saas_metrics.subscription_seats,
      -- Wave 2 & 3
      monthly_saas_metrics.umau_28_days_user,
      monthly_saas_metrics.action_monthly_active_users_project_repo_28_days_user,
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
      monthly_saas_metrics.jira_imports_28_days_event,
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
      monthly_saas_metrics.is_latest_data
    FROM monthly_saas_metrics
    LEFT JOIN billing_accounts
      ON monthly_saas_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN crm_accounts
      ON billing_accounts.dim_crm_account_id = crm_accounts.dim_crm_account_id
    LEFT JOIN subscriptions
      ON monthly_saas_metrics.dim_subscription_id = subscriptions.dim_subscription_id 
      AND IFNULL(monthly_saas_metrics.ping_created_at::DATE, DATEADD('day', -1, monthly_saas_metrics.snapshot_month))
      = TO_DATE(TO_CHAR(subscriptions.snapshot_id), 'YYYYMMDD')
    -- LEFT JOIN location_country
    --   ON monthly_saas_metrics.dim_location_country_id = location_country.dim_location_country_id

), unioned AS (

    SELECT *
    FROM sm_paid_user_metrics

    UNION ALL

    SELECT *
    FROM saas_paid_user_metrics

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@ischweickartDD",
    updated_by="@chrissharp",
    created_date="2021-06-11",
    updated_date="2021-10-21"
) }}
