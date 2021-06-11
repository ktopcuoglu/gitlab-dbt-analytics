{{config({
    "schema": "pumps"
  })
}}

{{ simple_cte([
    ('free_user_metrics', 'fct_product_usage_free_user_metrics_monthly'),
    ('crm_accounts', 'dim_crm_account')
]) }}

, joined AS (

    SELECT
      free_user_metrics.reporting_month,
      free_user_metrics.dim_namespace_id,
      free_user_metrics.uuid,
      free_user_metrics.hostname,
      free_user_metrics.delivery_type,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      crm_account_name,
      parent_crm_account_name,
      free_user_metrics.ping_date,
      -- Wave 2 & 3
      free_user_metrics.umau_28_days_user,
      free_user_metrics.action_monthly_active_users_project_repo_28_days_user,
      free_user_metrics.merge_requests_28_days_user,
      free_user_metrics.projects_with_repositories_enabled_28_days_user,
      free_user_metrics.commit_comment_all_time_event,
      free_user_metrics.source_code_pushes_all_time_event,
      free_user_metrics.ci_pipelines_28_days_user,
      free_user_metrics.ci_internal_pipelines_28_days_user,
      free_user_metrics.ci_builds_28_days_user,
      free_user_metrics.ci_builds_all_time_user,
      free_user_metrics.ci_builds_all_time_event,
      free_user_metrics.ci_runners_all_time_event,
      free_user_metrics.auto_devops_enabled_all_time_event,
      free_user_metrics.gitlab_shared_runners_enabled,
      free_user_metrics.container_registry_enabled,
      free_user_metrics.template_repositories_all_time_event,
      free_user_metrics.ci_pipeline_config_repository_28_days_user,
      free_user_metrics.user_unique_users_all_secure_scanners_28_days_user,
      free_user_metrics.user_sast_jobs_28_days_user,
      free_user_metrics.user_dast_jobs_28_days_user,
      free_user_metrics.user_dependency_scanning_jobs_28_days_user,
      free_user_metrics.user_license_management_jobs_28_days_user,
      free_user_metrics.user_secret_detection_jobs_28_days_user,
      free_user_metrics.user_container_scanning_jobs_28_days_user,
      free_user_metrics.object_store_packages_enabled,
      free_user_metrics.projects_with_packages_all_time_event,
      free_user_metrics.projects_with_packages_28_days_user,
      free_user_metrics.deployments_28_days_user,
      free_user_metrics.releases_28_days_user,
      free_user_metrics.epics_28_days_user,
      free_user_metrics.issues_28_days_user,
      -- Wave 3.1
      free_user_metrics.ci_internal_pipelines_all_time_event,
      free_user_metrics.ci_external_pipelines_all_time_event,
      free_user_metrics.merge_requests_all_time_event,
      free_user_metrics.todos_all_time_event,
      free_user_metrics.epics_all_time_event,
      free_user_metrics.issues_all_time_event,
      free_user_metrics.projects_all_time_event,
      free_user_metrics.deployments_28_days_event,
      free_user_metrics.packages_28_days_event,
      free_user_metrics.sast_jobs_all_time_event,
      free_user_metrics.dast_jobs_all_time_event,
      free_user_metrics.dependency_scanning_jobs_all_time_event,
      free_user_metrics.license_management_jobs_all_time_event,
      free_user_metrics.secret_detection_jobs_all_time_event,
      free_user_metrics.container_scanning_jobs_all_time_event,
      free_user_metrics.projects_jenkins_active_all_time_event,
      free_user_metrics.projects_bamboo_active_all_time_event,
      free_user_metrics.projects_jira_active_all_time_event,
      free_user_metrics.projects_drone_ci_active_all_time_event,
      free_user_metrics.jira_imports_28_days_event,
      free_user_metrics.projects_github_active_all_time_event,
      free_user_metrics.projects_jira_server_active_all_time_event,
      free_user_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      free_user_metrics.projects_with_repositories_enabled_all_time_event,
      free_user_metrics.protected_branches_all_time_event,
      free_user_metrics.remote_mirrors_all_time_event,
      free_user_metrics.projects_enforcing_code_owner_approval_28_days_user,
      free_user_metrics.project_clusters_enabled_28_days_user,
      free_user_metrics.analytics_28_days_user,
      free_user_metrics.issues_edit_28_days_user,
      free_user_metrics.user_packages_28_days_user,
      free_user_metrics.terraform_state_api_28_days_user,
      free_user_metrics.incident_management_28_days_user,
      -- Wave 3.2
      free_user_metrics.auto_devops_enabled,
      free_user_metrics.gitaly_clusters_instance,
      free_user_metrics.epics_deepest_relationship_level_instance,
      free_user_metrics.clusters_applications_cilium_all_time_event,
      free_user_metrics.network_policy_forwards_all_time_event,
      free_user_metrics.network_policy_drops_all_time_event,
      free_user_metrics.requirements_with_test_report_all_time_event,
      free_user_metrics.requirement_test_reports_ci_all_time_event,
      free_user_metrics.projects_imported_from_github_all_time_event,
      free_user_metrics.projects_jira_cloud_active_all_time_event,
      free_user_metrics.projects_jira_dvcs_server_active_all_time_event,
      free_user_metrics.service_desk_issues_all_time_event,
      free_user_metrics.ci_pipelines_all_time_user,
      free_user_metrics.service_desk_issues_28_days_user,
      free_user_metrics.projects_jira_active_28_days_user,
      free_user_metrics.projects_jira_dvcs_cloud_active_28_days_user,
      free_user_metrics.projects_jira_dvcs_server_active_28_days_user,
      free_user_metrics.merge_requests_with_required_code_owners_28_days_user,
      free_user_metrics.analytics_value_stream_28_days_event,
      free_user_metrics.code_review_user_approve_mr_28_days_user,
      free_user_metrics.epics_usage_28_days_user,
      free_user_metrics.ci_templates_usage_28_days_event,
      free_user_metrics.project_management_issue_milestone_changed_28_days_user,
      free_user_metrics.project_management_issue_iteration_changed_28_days_user
    FROM free_user_metrics
    LEFT JOIN crm_accounts
      ON free_user_metrics.dim_crm_account_id = crm_accounts.dim_crm_account_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-08",
    updated_date="2021-06-08"
) }}