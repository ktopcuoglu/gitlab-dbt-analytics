{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('monthly_metrics','fct_product_usage_wave_1_3_metrics_monthly'),
    ('dim_date','dim_date')
]) }}

, months AS (

    SELECT DISTINCT
      first_day_of_month,
      days_in_month_count
    FROM dim_date

), ping_ranges AS (

    SELECT DISTINCT
      dim_subscription_id,
      {{ usage_ping_month_range('commit_comment_all_time_event') }},
      {{ usage_ping_month_range('source_code_pushes_all_time_event') }},
      {{ usage_ping_month_range('ci_builds_all_time_event') }},
      {{ usage_ping_month_range('ci_runners_all_time_event') }},
      {{ usage_ping_month_range('template_repositories_all_time_event') }},
      {{ usage_ping_month_range('projects_with_packages_all_time_event') }},
      {{ usage_ping_month_range('auto_devops_enabled_all_time_event') }},
      {{ usage_ping_month_range('ci_internal_pipelines_all_time_event') }},
      {{ usage_ping_month_range('ci_external_pipelines_all_time_event') }},
      {{ usage_ping_month_range('merge_requests_all_time_event') }},
      {{ usage_ping_month_range('todos_all_time_event') }},
      {{ usage_ping_month_range('epics_all_time_event') }},
      {{ usage_ping_month_range('issues_all_time_event') }},
      {{ usage_ping_month_range('projects_all_time_event') }},
      {{ usage_ping_month_range('sast_jobs_all_time_event') }},
      {{ usage_ping_month_range('dast_jobs_all_time_event') }},
      {{ usage_ping_month_range('dependency_scanning_jobs_all_time_event') }},
      {{ usage_ping_month_range('license_management_jobs_all_time_event') }},
      {{ usage_ping_month_range('secret_detection_jobs_all_time_event') }},
      {{ usage_ping_month_range('container_scanning_jobs_all_time_event') }},
      {{ usage_ping_month_range('projects_jenkins_active_all_time_event') }},
      {{ usage_ping_month_range('projects_bamboo_active_all_time_event') }},
      {{ usage_ping_month_range('projects_jira_active_all_time_event') }},
      {{ usage_ping_month_range('projects_drone_ci_active_all_time_event') }},
      {{ usage_ping_month_range('projects_github_active_all_time_event') }},
      {{ usage_ping_month_range('projects_jira_server_active_all_time_event') }},
      {{ usage_ping_month_range('projects_jira_dvcs_cloud_active_all_time_event') }},
      {{ usage_ping_month_range('projects_with_repositories_enabled_all_time_event') }},
      {{ usage_ping_month_range('protected_branches_all_time_event') }},
      {{ usage_ping_month_range('remote_mirrors_all_time_event') }},
      {{ usage_ping_month_range('clusters_applications_cilium_all_time_event') }},
      {{ usage_ping_month_range('network_policy_forwards_all_time_event') }},
      {{ usage_ping_month_range('network_policy_drops_all_time_event') }},
      {{ usage_ping_month_range('requirements_with_test_report_all_time_event') }},
      {{ usage_ping_month_range('requirement_test_reports_ci_all_time_event') }},
      {{ usage_ping_month_range('projects_imported_from_github_all_time_event') }},
      {{ usage_ping_month_range('projects_jira_cloud_active_all_time_event') }},
      {{ usage_ping_month_range('projects_jira_dvcs_server_active_all_time_event') }},
      {{ usage_ping_month_range('service_desk_issues_all_time_event') }}
    FROM monthly_metrics

), diffs AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      ping_created_at::DATE - LAG(ping_created_at::DATE)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS days_since_last_ping,
      {{ usage_ping_over_ping_difference('commit_comment_all_time_event') }},
      {{ usage_ping_over_ping_difference('source_code_pushes_all_time_event') }},
      {{ usage_ping_over_ping_difference('ci_builds_all_time_event') }},
      {{ usage_ping_over_ping_difference('ci_runners_all_time_event') }},
      {{ usage_ping_over_ping_difference('template_repositories_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_with_packages_all_time_event') }},
      {{ usage_ping_over_ping_difference('auto_devops_enabled_all_time_event') }},
      {{ usage_ping_over_ping_difference('ci_internal_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_difference('ci_external_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_difference('merge_requests_all_time_event') }},
      {{ usage_ping_over_ping_difference('todos_all_time_event') }},
      {{ usage_ping_over_ping_difference('epics_all_time_event') }},
      {{ usage_ping_over_ping_difference('issues_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_all_time_event') }},
      {{ usage_ping_over_ping_difference('sast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('dast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('dependency_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('license_management_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('secret_detection_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('container_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jenkins_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_bamboo_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jira_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_drone_ci_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_github_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jira_server_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jira_dvcs_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_with_repositories_enabled_all_time_event') }},
      {{ usage_ping_over_ping_difference('protected_branches_all_time_event') }},
      {{ usage_ping_over_ping_difference('remote_mirrors_all_time_event') }},
      {{ usage_ping_over_ping_difference('clusters_applications_cilium_all_time_event') }},
      {{ usage_ping_over_ping_difference('network_policy_forwards_all_time_event') }},
      {{ usage_ping_over_ping_difference('network_policy_drops_all_time_event') }},
      {{ usage_ping_over_ping_difference('requirements_with_test_report_all_time_event') }},
      {{ usage_ping_over_ping_difference('requirement_test_reports_ci_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_imported_from_github_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jira_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('projects_jira_dvcs_server_active_all_time_event') }},
      {{ usage_ping_over_ping_difference('service_desk_issues_all_time_event') }}
    FROM monthly_metrics

), smoothed_diffs AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      days_since_last_ping,
      months.days_in_month_count,
      {{ usage_ping_over_ping_smoothed('commit_comment_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('source_code_pushes_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('ci_builds_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('ci_runners_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('template_repositories_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_with_packages_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('auto_devops_enabled_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('ci_internal_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('ci_external_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('merge_requests_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('todos_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('epics_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('issues_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('sast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('dast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('dependency_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('license_management_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('secret_detection_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('container_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jenkins_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_bamboo_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jira_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_drone_ci_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_github_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jira_server_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jira_dvcs_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_with_repositories_enabled_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('protected_branches_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('remote_mirrors_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('clusters_applications_cilium_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('network_policy_forwards_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('network_policy_drops_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('requirements_with_test_report_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('requirement_test_reports_ci_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_imported_from_github_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jira_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('projects_jira_dvcs_server_active_all_time_event') }},
      {{ usage_ping_over_ping_smoothed('service_desk_issues_all_time_event') }}
    FROM diffs
    INNER JOIN months
      ON diffs.snapshot_month = months.first_day_of_month

), final AS (

    SELECT
      smoothed_diffs.dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      {{ usage_ping_over_ping_estimated('commit_comment_all_time_event') }},
      {{ usage_ping_over_ping_estimated('source_code_pushes_all_time_event') }},
      {{ usage_ping_over_ping_estimated('ci_builds_all_time_event') }},
      {{ usage_ping_over_ping_estimated('ci_runners_all_time_event') }},
      {{ usage_ping_over_ping_estimated('template_repositories_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_with_packages_all_time_event') }},
      {{ usage_ping_over_ping_estimated('auto_devops_enabled_all_time_event') }},
      {{ usage_ping_over_ping_estimated('ci_internal_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_estimated('ci_external_pipelines_all_time_event') }},
      {{ usage_ping_over_ping_estimated('merge_requests_all_time_event') }},
      {{ usage_ping_over_ping_estimated('todos_all_time_event') }},
      {{ usage_ping_over_ping_estimated('epics_all_time_event') }},
      {{ usage_ping_over_ping_estimated('issues_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_all_time_event') }},
      {{ usage_ping_over_ping_estimated('sast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('dast_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('dependency_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('license_management_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('secret_detection_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('container_scanning_jobs_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jenkins_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_bamboo_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jira_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_drone_ci_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_github_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jira_server_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jira_dvcs_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_with_repositories_enabled_all_time_event') }},
      {{ usage_ping_over_ping_estimated('protected_branches_all_time_event') }},
      {{ usage_ping_over_ping_estimated('remote_mirrors_all_time_event') }},
      {{ usage_ping_over_ping_estimated('clusters_applications_cilium_all_time_event') }},
      {{ usage_ping_over_ping_estimated('network_policy_forwards_all_time_event') }},
      {{ usage_ping_over_ping_estimated('network_policy_drops_all_time_event') }},
      {{ usage_ping_over_ping_estimated('requirements_with_test_report_all_time_event') }},
      {{ usage_ping_over_ping_estimated('requirement_test_reports_ci_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_imported_from_github_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jira_cloud_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('projects_jira_dvcs_server_active_all_time_event') }},
      {{ usage_ping_over_ping_estimated('service_desk_issues_all_time_event') }}
    FROM smoothed_diffs
    LEFT JOIN ping_ranges
      ON smoothed_diffs.dim_subscription_id = ping_ranges.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-04",
    updated_date="2021-04-13"
) }}