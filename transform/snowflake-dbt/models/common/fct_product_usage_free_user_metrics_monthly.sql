{{ config(
    tags=["mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{{ simple_cte([
    ('saas_free_users','prep_saas_usage_ping_free_user_metrics'),
    ('sm_free_users','prep_usage_ping_free_user_metrics'),
    ('smau', 'fct_usage_ping_subscription_mapped_smau')
]) }}

, smau_convert AS (

    SELECT distinct
      uuid,
      hostname,
      snapshot_month,
      {{ convert_variant_to_number_field('manage_analytics_total_unique_counts_monthly') }}                                         AS analytics_28_days_user,                   
      {{ convert_variant_to_number_field('plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly') }}          AS issues_edit_28_days_user,
      {{ convert_variant_to_number_field('package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly') }}   AS user_packages_28_days_user, 
      {{ convert_variant_to_number_field('configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly') }}    AS terraform_state_api_28_days_user,
      {{ convert_variant_to_number_field('monitor_incident_management_activer_user_28_days') }}                                     AS incident_management_28_days_user
    FROM smau

), sm_free_user_metrics AS (

    SELECT
      sm_free_users.ping_created_at_month::DATE                                                 AS reporting_month,
      NULL                                                                                      AS dim_namespace_id,
      sm_free_users.uuid,
      sm_free_users.hostname,
      'Self-Managed'                                                                            AS delivery_type,
      sm_free_users.cleaned_version,
      sm_free_users.dim_crm_account_id,
      sm_free_users.ping_created_at::DATE                                                       AS ping_date,
      sm_free_users.umau_28_days_user,
      sm_free_users.action_monthly_active_users_project_repo_28_days_user,
      sm_free_users.merge_requests_28_days_user,
      sm_free_users.projects_with_repositories_enabled_28_days_user,
      sm_free_users.commit_comment_all_time_event,
      sm_free_users.source_code_pushes_all_time_event,
      sm_free_users.ci_pipelines_28_days_user,
      sm_free_users.ci_internal_pipelines_28_days_user,
      sm_free_users.ci_builds_28_days_user,
      sm_free_users.ci_builds_all_time_user,
      sm_free_users.ci_builds_all_time_event,
      sm_free_users.ci_runners_all_time_event,
      sm_free_users.auto_devops_enabled_all_time_event,
      sm_free_users.gitlab_shared_runners_enabled,
      sm_free_users.container_registry_enabled,
      sm_free_users.template_repositories_all_time_event,
      sm_free_users.ci_pipeline_config_repository_28_days_user,
      sm_free_users.user_unique_users_all_secure_scanners_28_days_user,
      sm_free_users.user_sast_jobs_28_days_user,
      sm_free_users.user_dast_jobs_28_days_user,
      sm_free_users.user_dependency_scanning_jobs_28_days_user,
      sm_free_users.user_license_management_jobs_28_days_user,
      sm_free_users.user_secret_detection_jobs_28_days_user,
      sm_free_users.user_container_scanning_jobs_28_days_user,
      sm_free_users.object_store_packages_enabled,
      sm_free_users.projects_with_packages_all_time_event,
      sm_free_users.projects_with_packages_28_days_user,
      sm_free_users.deployments_28_days_user,
      sm_free_users.releases_28_days_user,
      sm_free_users.epics_28_days_user,
      sm_free_users.issues_28_days_user,
      -- Wave 3.1
      sm_free_users.ci_internal_pipelines_all_time_event,
      sm_free_users.ci_external_pipelines_all_time_event,
      sm_free_users.merge_requests_all_time_event,
      sm_free_users.todos_all_time_event,
      sm_free_users.epics_all_time_event,
      sm_free_users.issues_all_time_event,
      sm_free_users.projects_all_time_event,
      sm_free_users.deployments_28_days_event,
      sm_free_users.packages_28_days_event,
      sm_free_users.sast_jobs_all_time_event,
      sm_free_users.dast_jobs_all_time_event,
      sm_free_users.dependency_scanning_jobs_all_time_event,
      sm_free_users.license_management_jobs_all_time_event,
      sm_free_users.secret_detection_jobs_all_time_event,
      sm_free_users.container_scanning_jobs_all_time_event,
      sm_free_users.projects_jenkins_active_all_time_event,
      sm_free_users.projects_bamboo_active_all_time_event,
      sm_free_users.projects_jira_active_all_time_event,
      sm_free_users.projects_drone_ci_active_all_time_event,
      sm_free_users.jira_imports_28_days_event,
      sm_free_users.projects_github_active_all_time_event,
      sm_free_users.projects_jira_server_active_all_time_event,
      sm_free_users.projects_jira_dvcs_cloud_active_all_time_event,
      sm_free_users.projects_with_repositories_enabled_all_time_event,
      sm_free_users.protected_branches_all_time_event,
      sm_free_users.remote_mirrors_all_time_event,
      sm_free_users.projects_enforcing_code_owner_approval_28_days_user,
      sm_free_users.project_clusters_enabled_28_days_user,
      {{ null_negative_numbers('smau_convert.analytics_28_days_user') }}                     AS analytics_28_days_user,
      {{ null_negative_numbers('smau_convert.issues_edit_28_days_user') }}                   AS issues_edit_28_days_user,
      {{ null_negative_numbers('smau_convert.user_packages_28_days_user') }}                 AS user_packages_28_days_user,
      {{ null_negative_numbers('smau_convert.terraform_state_api_28_days_user') }}           AS terraform_state_api_28_days_user,
      {{ null_negative_numbers('smau_convert.incident_management_28_days_user') }}           AS incident_management_28_days_user,
      -- Wave 3.2
      sm_free_users.auto_devops_enabled,
      sm_free_users.gitaly_clusters_instance,
      sm_free_users.epics_deepest_relationship_level_instance,
      sm_free_users.clusters_applications_cilium_all_time_event,
      sm_free_users.network_policy_forwards_all_time_event,
      sm_free_users.network_policy_drops_all_time_event,
      sm_free_users.requirements_with_test_report_all_time_event,
      sm_free_users.requirement_test_reports_ci_all_time_event,
      sm_free_users.projects_imported_from_github_all_time_event,
      sm_free_users.projects_jira_cloud_active_all_time_event,
      sm_free_users.projects_jira_dvcs_server_active_all_time_event,
      sm_free_users.service_desk_issues_all_time_event,
      sm_free_users.ci_pipelines_all_time_user,
      sm_free_users.service_desk_issues_28_days_user,
      sm_free_users.projects_jira_active_28_days_user,
      sm_free_users.projects_jira_dvcs_cloud_active_28_days_user,
      sm_free_users.projects_jira_dvcs_server_active_28_days_user,
      sm_free_users.merge_requests_with_required_code_owners_28_days_user,
      sm_free_users.analytics_value_stream_28_days_event,
      sm_free_users.code_review_user_approve_mr_28_days_user,
      sm_free_users.epics_usage_28_days_user,
      sm_free_users.ci_templates_usage_28_days_event,
      sm_free_users.project_management_issue_milestone_changed_28_days_user,
      sm_free_users.project_management_issue_iteration_changed_28_days_user,
      IFF(ROW_NUMBER() OVER (PARTITION BY sm_free_users.uuid, sm_free_users.hostname
                             ORDER BY sm_free_users.ping_created_at DESC
                            ) = 1,
          TRUE, FALSE)                                                                          AS is_latest_data
    FROM sm_free_users
    LEFT JOIN smau_convert
      ON sm_free_users.uuid = smau_convert.uuid
      AND sm_free_users.hostname = smau_convert.hostname
      AND sm_free_users.ping_created_at_month = smau_convert.snapshot_month
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        sm_free_users.uuid,
        sm_free_users.hostname,
        sm_free_users.ping_created_at_month
      ORDER BY sm_free_users.ping_created_at DESC
    ) = 1

), saas_free_user_metrics AS (

    SELECT
      reporting_month,
      dim_namespace_id,
      NULL                                                                                      AS uuid,
      NULL                                                                                      AS hostname,
      'SaaS'                                                                                    AS delivery_type,
      NULL                                                                                      AS cleaned_version,
      dim_crm_account_id,
      ping_date::DATE                                                                           AS ping_date,
      -- Wave 2 & 3
      "usage_activity_by_stage_monthly.manage.events"                                           AS umau_28_days_user,
      "usage_activity_by_stage_monthly.create.action_monthly_active_users_project_repo"         AS action_monthly_active_users_project_repo_28_days_user,
      "usage_activity_by_stage_monthly.create.merge_requests"                                   AS merge_requests_28_days_user,
      "usage_activity_by_stage_monthly.create.projects_with_repositories_enabled"               AS projects_with_repositories_enabled_28_days_user,
      "counts.commit_comment"                                                                   AS commit_comment_all_time_event,
      "counts.source_code_pushes"                                                               AS source_code_pushes_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipelines"                                     AS ci_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_internal_pipelines"                            AS ci_internal_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_builds"                                        AS ci_builds_28_days_user,
      "usage_activity_by_stage.verify.ci_builds"                                                AS ci_builds_all_time_user,
      "counts.ci_builds"                                                                        AS ci_builds_all_time_event,
      "counts.ci_runners"                                                                       AS ci_runners_all_time_event,
      "counts.auto_devops_enabled"                                                              AS auto_devops_enabled_all_time_event,
      "gitlab_shared_runners_enabled"                                                           AS gitlab_shared_runners_enabled,
      "container_registry_enabled"                                                              AS container_registry_enabled,
      "counts.template_repositories"                                                            AS template_repositories_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipeline_config_repository"                    AS ci_pipeline_config_repository_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_unique_users_all_secure_scanners"            AS user_unique_users_all_secure_scanners_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_sast_jobs"                                   AS user_sast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dast_jobs"                                   AS user_dast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dependency_scanning_jobs"                    AS user_dependency_scanning_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_license_management_jobs"                     AS user_license_management_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_secret_detection_jobs"                       AS user_secret_detection_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_container_scanning_jobs"                     AS user_container_scanning_jobs_28_days_user,
      "object_store.packages.enabled"                                                           AS object_store_packages_enabled,
      "counts.projects_with_packages"                                                           AS projects_with_packages_all_time_event,
      "usage_activity_by_stage_monthly.package.projects_with_packages"                          AS projects_with_packages_28_days_user,
      "usage_activity_by_stage_monthly.release.deployments"                                     AS deployments_28_days_user,
      "usage_activity_by_stage_monthly.release.releases"                                        AS releases_28_days_user,
      "usage_activity_by_stage_monthly.plan.epics"                                              AS epics_28_days_user,
      "usage_activity_by_stage_monthly.plan.issues"                                             AS issues_28_days_user,
      -- Wave 3.1
      "counts.ci_internal_pipelines"                                                            AS ci_internal_pipelines_all_time_event,
      "counts.ci_external_pipelines"                                                            AS ci_external_pipelines_all_time_event,
      "counts.merge_requests"                                                                   AS merge_requests_all_time_event,
      "counts.todos"                                                                            AS todos_all_time_event,
      "counts.epics"                                                                            AS epics_all_time_event,
      "counts.issues"                                                                           AS issues_all_time_event,
      "counts.projects"                                                                         AS projects_all_time_event,
      "counts_monthly.deployments"                                                              AS deployments_28_days_event,
      "counts_monthly.packages"                                                                 AS packages_28_days_event,
      "counts.sast_jobs"                                                                        AS sast_jobs_all_time_event,
      "counts.dast_jobs"                                                                        AS dast_jobs_all_time_event,
      "counts.dependency_scanning_jobs"                                                         AS dependency_scanning_jobs_all_time_event,
      "counts.license_management_jobs"                                                          AS license_management_jobs_all_time_event,
      "counts.secret_detection_jobs"                                                            AS secret_detection_jobs_all_time_event,
      "counts.container_scanning_jobs"                                                          AS container_scanning_jobs_all_time_event,
      "counts.projects_jenkins_active"                                                          AS projects_jenkins_active_all_time_event,
      "counts.projects_bamboo_active"                                                           AS projects_bamboo_active_all_time_event,
      "counts.projects_jira_active"                                                             AS projects_jira_active_all_time_event,
      "counts.projects_drone_ci_active"                                                         AS projects_drone_ci_active_all_time_event,
      "usage_activity_by_stage_monthly.manage.issues_imported.jira"                             AS jira_imports_28_days_event,
      "counts.projects_github_active"                                                           AS projects_github_active_all_time_event,
      "counts.projects_jira_server_active"                                                      AS projects_jira_server_active_all_time_event,
      "counts.projects_jira_dvcs_cloud_active"                                                  AS projects_jira_dvcs_cloud_active_all_time_event,
      "counts.projects_with_repositories_enabled"                                               AS projects_with_repositories_enabled_all_time_event,
      "counts.protected_branches"                                                               AS protected_branches_all_time_event,
      "counts.remote_mirrors"                                                                   AS remote_mirrors_all_time_event,
      "usage_activity_by_stage.create.projects_enforcing_code_owner_approval"                   AS projects_enforcing_code_owner_approval_28_days_user,
      "usage_activity_by_stage_monthly.configure.project_clusters_enabled"                      AS project_clusters_enabled_28_days_user,
      "analytics_total_unique_counts_monthly"                                                   AS analytics_28_days_user,
      "redis_hll_counters.issues_edit.issues_edit_total_unique_counts_monthly"                  AS issues_edit_28_days_user,
      "redis_hll_counters.user_packages.user_packages_total_unique_counts_monthly"              AS user_packages_28_days_user,
      "redis_hll_counters.terraform.p_terraform_state_api_unique_users_monthly"                 AS terraform_state_api_28_days_user,
      "redis_hll_counters.incident_management.incident_management_total_unique_counts_monthly"  AS incident_management_28_days_user,
      -- Wave 3.2
      "instance_auto_devops_enabled"                                                            AS auto_devops_enabled,
      "gitaly.clusters"                                                                         AS gitaly_clusters_instance,
      "counts.epics_deepest_relationship_level"                                                 AS epics_deepest_relationship_level_instance,
      "counts.clusters_applications_cilium"                                                     AS clusters_applications_cilium_all_time_event,
      "counts.network_policy_forwards"                                                          AS network_policy_forwards_all_time_event,
      "counts.network_policy_drops"                                                             AS network_policy_drops_all_time_event,
      "counts.requirements_with_test_report"                                                    AS requirements_with_test_report_all_time_event,
      "counts.requirement_test_reports_ci"                                                      AS requirement_test_reports_ci_all_time_event,
      "counts.projects_imported_from_github"                                                    AS projects_imported_from_github_all_time_event,
      "counts.projects_jira_cloud_active"                                                       AS projects_jira_cloud_active_all_time_event,
      "counts.projects_jira_dvcs_server_active"                                                 AS projects_jira_dvcs_server_active_all_time_event,
      "counts.service_desk_issues"                                                              AS service_desk_issues_all_time_event,
      "usage_activity_by_stage.verify.ci_pipelines"                                             AS ci_pipelines_all_time_user,
      "usage_activity_by_stage_monthly.plan.service_desk_issues"                                AS service_desk_issues_28_days_user,
      "usage_activity_by_stage_monthly.plan.projects_jira_active"                               AS projects_jira_active_28_days_user,
      "usage_activity_by_stage_monthly.plan.projects_jira_dvcs_cloud_active"                    AS projects_jira_dvcs_cloud_active_28_days_user,
      "usage_activity_by_stage_monthly.plan.projects_jira_dvcs_server_active"                   AS projects_jira_dvcs_server_active_28_days_user,
      "usage_activity_by_stage_monthly.create.merge_requests_with_required_codeowners"          AS merge_requests_with_required_code_owners_28_days_user,
      "redis_hll_counters.analytics.g_analytics_valuestream_monthly"                            AS analytics_value_stream_28_days_event,
      "redis_hll_counters.code_review.i_code_review_user_approve_mr_monthly"                    AS code_review_user_approve_mr_28_days_user,
      "redis_hll_counters.epics_usage.epics_usage_total_unique_counts_monthly"                  AS epics_usage_28_days_user,
      "redis_hll_counters.ci_templates.ci_templates_total_unique_counts_monthly"                AS ci_templates_usage_28_days_event,
      "redis_hll_counters.issues_edit.g_project_management_issue_milestone_changed_monthly"     AS project_management_issue_milestone_changed_28_days_user,
      "redis_hll_counters.issues_edit.g_project_management_issue_iteration_changed_monthly"     AS project_management_issue_iteration_changed_28_days_user,
      IFF(ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY reporting_month DESC) = 1,
          TRUE, FALSE)                                                                          AS is_latest_data
    FROM saas_free_users

), unioned AS (

    SELECT *
    FROM sm_free_user_metrics

    UNION ALL

    SELECT *
    FROM saas_free_user_metrics

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@ischweickartDD",
    updated_by="@snalamaru",
    created_date="2021-06-08",
    updated_date="2021-09-28"
) }}