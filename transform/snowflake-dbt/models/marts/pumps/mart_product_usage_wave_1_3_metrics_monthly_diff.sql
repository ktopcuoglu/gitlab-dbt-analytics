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
      MIN(IFF(commit_comment_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS commit_comment_first_ping_month,
      MAX(IFF(commit_comment_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS commit_comment_last_ping_month,  
      MIN(IFF(source_code_pushes_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS source_code_pushes_first_ping_month,
      MAX(IFF(source_code_pushes_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS source_code_pushes_last_ping_month,  
      MIN(IFF(ci_builds_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_builds_first_ping_month,
      MAX(IFF(ci_builds_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_builds_last_ping_month,  
      MIN(IFF(ci_builds_all_time_user IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_build_users_first_ping_month,
      MAX(IFF(ci_builds_all_time_user IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_build_users_event_last_ping_month,   
      MIN(IFF(ci_runners_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_runners_first_ping_month,
      MAX(IFF(ci_runners_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_runners_last_ping_month,  
      MIN(IFF(template_repositories_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS template_repositories_first_ping_month,
      MAX(IFF(template_repositories_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS template_repositories_last_ping_month,  
      MIN(IFF(projects_with_packages_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_packages_first_ping_month,
      MAX(IFF(projects_with_packages_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_packages_last_ping_month,  
      MIN(IFF(auto_devops_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS auto_devops_enabled_first_ping_month,
      MAX(IFF(auto_devops_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS auto_devops_enabled_last_ping_month,
      MIN(IFF(ci_internal_pipelines_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_internal_pipelines_first_ping_month,
      MAX(IFF(ci_internal_pipelines_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_internal_pipelines_last_ping_month,
      MIN(IFF(ci_external_pipelines_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_external_pipelines_first_ping_month,
      MAX(IFF(ci_external_pipelines_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS ci_external_pipelines_last_ping_month,
      MIN(IFF(merge_requests_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS merge_requests_first_ping_month,
      MAX(IFF(merge_requests_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS merge_requests_last_ping_month,
      MIN(IFF(todos_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS todos_first_ping_month,
      MAX(IFF(todos_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS todos_last_ping_month,
      MIN(IFF(epics_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS epics_first_ping_month,
      MAX(IFF(epics_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS epics_last_ping_month,
      MIN(IFF(issues_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS issues_first_ping_month,
      MAX(IFF(issues_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS issues_last_ping_month,
      MIN(IFF(projects_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_first_ping_month,
      MAX(IFF(projects_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_last_ping_month,
      MIN(IFF(sast_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS sast_jobs_first_ping_month,
      MAX(IFF(sast_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS sast_jobs_last_ping_month,
      MIN(IFF(dast_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS dast_jobs_first_ping_month,
      MAX(IFF(dast_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS dast_jobs_last_ping_month,
      MIN(IFF(dependency_scanning_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS dependency_scanning_jobs_first_ping_month,
      MAX(IFF(dependency_scanning_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS dependency_scanning_jobs_last_ping_month,
      MIN(IFF(license_management_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS license_management_jobs_first_ping_month,
      MAX(IFF(license_management_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS license_management_jobs_last_ping_month,
      MIN(IFF(secret_detection_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS secret_detection_jobs_first_ping_month,
      MAX(IFF(secret_detection_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS secret_detection_jobs_last_ping_month,
      MIN(IFF(container_scanning_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS container_scanning_jobs_first_ping_month,
      MAX(IFF(container_scanning_jobs_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS container_scanning_jobs_last_ping_month,
      MIN(IFF(projects_jenkins_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jenkins_active_first_ping_month,
      MAX(IFF(projects_jenkins_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jenkins_active_last_ping_month,
      MIN(IFF(projects_bamboo_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_bamboo_active_first_ping_month,
      MAX(IFF(projects_bamboo_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_bamboo_active_last_ping_month,
      MIN(IFF(projects_jira_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_active_first_ping_month,
      MAX(IFF(projects_jira_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_active_last_ping_month,
      MIN(IFF(projects_drone_ci_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_drone_ci_active_first_ping_month,
      MAX(IFF(projects_drone_ci_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_drone_ci_active_last_ping_month,
      MIN(IFF(projects_github_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_github_active_first_ping_month,
      MAX(IFF(projects_github_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_github_active_last_ping_month,
      MIN(IFF(projects_jira_server_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_server_active_first_ping_month,
      MAX(IFF(projects_jira_server_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_server_active_last_ping_month,
      MIN(IFF(projects_jira_dvcs_cloud_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_dvcs_cloud_active_first_ping_month,
      MAX(IFF(projects_jira_dvcs_cloud_active_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_jira_dvcs_cloud_active_last_ping_month,
      MIN(IFF(projects_with_repositories_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_repositories_enabled_first_ping_month,
      MAX(IFF(projects_with_repositories_enabled_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS projects_with_repositories_enabled_last_ping_month,
      MIN(IFF(protected_branches_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS protected_branches_first_ping_month,
      MAX(IFF(protected_branches_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS protected_branches_last_ping_month,
      MIN(IFF(remote_mirrors_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS remote_mirrors_first_ping_month,
      MAX(IFF(remote_mirrors_all_time_event IS NOT NULL, snapshot_month, NULL))
        OVER (PARTITION BY dim_subscription_id)                                         AS remote_mirrors_last_ping_month
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
      commit_comment_all_time_event,
      commit_comment_all_time_event - LAG(commit_comment_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS commit_comment_since_last_ping,
      source_code_pushes_all_time_event,
      source_code_pushes_all_time_event - LAG(source_code_pushes_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS source_code_pushes_since_last_ping,
      ci_builds_all_time_event,
      ci_builds_all_time_event - LAG(ci_builds_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_builds_since_last_ping,
      ci_runners_all_time_event,
      ci_runners_all_time_event - LAG(ci_runners_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_runners_since_last_ping,
      template_repositories_all_time_event,
      template_repositories_all_time_event - LAG(template_repositories_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS template_repositories_since_last_ping,
      projects_with_packages_all_time_event,
      projects_with_packages_all_time_event - LAG(projects_with_packages_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_with_packages_since_last_ping,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_all_time_event - LAG(auto_devops_enabled_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS auto_devops_enabled_since_last_ping,
      ci_builds_all_time_user,
      ci_builds_all_time_user - LAG(ci_builds_all_time_user)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_build_users_since_last_ping,
      ci_internal_pipelines_all_time_event,
      ci_internal_pipelines_all_time_event - LAG(ci_internal_pipelines_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_internal_pipelines_since_last_ping,
      ci_external_pipelines_all_time_event,
      ci_external_pipelines_all_time_event - LAG(ci_external_pipelines_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS ci_external_pipelines_since_last_ping,
      merge_requests_all_time_event,
      merge_requests_all_time_event - LAG(merge_requests_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS merge_requests_since_last_ping,
      todos_all_time_event,
      todos_all_time_event - LAG(todos_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS todos_since_last_ping,
      epics_all_time_event,
      epics_all_time_event - LAG(epics_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS epics_since_last_ping,
      issues_all_time_event,
      issues_all_time_event - LAG(issues_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS issues_since_last_ping,
      projects_all_time_event,
      projects_all_time_event - LAG(projects_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_since_last_ping,
      sast_jobs_all_time_event,
      sast_jobs_all_time_event - LAG(sast_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS sast_jobs_since_last_ping,
      dast_jobs_all_time_event,
      dast_jobs_all_time_event - LAG(dast_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS dast_jobs_since_last_ping,
      dependency_scanning_jobs_all_time_event,
      dependency_scanning_jobs_all_time_event - LAG(dependency_scanning_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS dependency_scanning_jobs_since_last_ping,
      license_management_jobs_all_time_event,
      license_management_jobs_all_time_event - LAG(license_management_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS license_management_jobs_since_last_ping,
      secret_detection_jobs_all_time_event,
      secret_detection_jobs_all_time_event - LAG(secret_detection_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS secret_detection_jobs_since_last_ping,
      container_scanning_jobs_all_time_event,
      container_scanning_jobs_all_time_event - LAG(container_scanning_jobs_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS container_scanning_jobs_since_last_ping,
      projects_jenkins_active_all_time_event,
      projects_jenkins_active_all_time_event - LAG(projects_jenkins_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_jenkins_active_since_last_ping,
      projects_bamboo_active_all_time_event,
      projects_bamboo_active_all_time_event - LAG(projects_bamboo_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_bamboo_active_since_last_ping,
      projects_jira_active_all_time_event,
      projects_jira_active_all_time_event - LAG(projects_jira_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_jira_active_since_last_ping,
      projects_drone_ci_active_all_time_event,
      projects_drone_ci_active_all_time_event - LAG(projects_drone_ci_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_drone_ci_active_since_last_ping,
      projects_github_active_all_time_event,
      projects_github_active_all_time_event - LAG(projects_github_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_github_active_since_last_ping,
      projects_jira_server_active_all_time_event,
      projects_jira_server_active_all_time_event - LAG(projects_jira_server_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_jira_server_active_since_last_ping,
      projects_jira_dvcs_cloud_active_all_time_event,
      projects_jira_dvcs_cloud_active_all_time_event - LAG(projects_jira_dvcs_cloud_active_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_jira_dvcs_cloud_active_since_last_ping,
      projects_with_repositories_enabled_all_time_event,
      projects_with_repositories_enabled_all_time_event - LAG(projects_with_repositories_enabled_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS projects_with_repositories_enabled_since_last_ping,
      protected_branches_all_time_event,
      protected_branches_all_time_event - LAG(protected_branches_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS protected_branches_since_last_ping,
      remote_mirrors_all_time_event,
      remote_mirrors_all_time_event - LAG(remote_mirrors_all_time_event)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month)    AS remote_mirrors_since_last_ping
    FROM monthly_metrics

), smoothed_diffs AS (

    SELECT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      snapshot_month,
      uuid,
      hostname,
      commit_comment_all_time_event,
      commit_comment_since_last_ping,
      FIRST_VALUE(commit_comment_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS commit_comment_per_day,
      (commit_comment_per_day * days_in_month_count)::INT                               AS commit_comment_smoothed,
      source_code_pushes_all_time_event,
      source_code_pushes_since_last_ping,
      FIRST_VALUE(source_code_pushes_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS source_code_pushes_per_day,
      (source_code_pushes_per_day * days_in_month_count)::INT                           AS source_code_pushes_smoothed,
      ci_builds_all_time_event,
      ci_builds_since_last_ping,
      FIRST_VALUE(ci_builds_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_builds_per_day,
      (ci_builds_per_day * days_in_month_count)::INT                                    AS ci_builds_smoothed,
      ci_runners_all_time_event,
      ci_runners_since_last_ping,
      FIRST_VALUE(ci_runners_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_runners_per_day,
      (ci_runners_per_day * days_in_month_count)::INT                                   AS ci_runners_smoothed,
      template_repositories_all_time_event,
      template_repositories_since_last_ping,
      FIRST_VALUE(template_repositories_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS template_repositories_per_day,
      (template_repositories_per_day * days_in_month_count)::INT                        AS template_repositories_smoothed,
      projects_with_packages_all_time_event,
      projects_with_packages_since_last_ping,
      FIRST_VALUE(projects_with_packages_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_with_packages_per_day,
      (projects_with_packages_per_day * days_in_month_count)::INT                       AS projects_with_packages_smoothed,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_since_last_ping,
      FIRST_VALUE(auto_devops_enabled_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS auto_devops_enabled_per_day,
      (auto_devops_enabled_per_day * days_in_month_count)::INT                          AS auto_devops_enabled_smoothed,
      ci_builds_all_time_user,
      ci_build_users_since_last_ping,
      ci_internal_pipelines_all_time_event,
      ci_internal_pipelines_since_last_ping,
      FIRST_VALUE(ci_internal_pipelines_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_internal_pipelines_per_day,
      (ci_internal_pipelines_per_day * days_in_month_count)::INT                        AS ci_internal_pipelines_smoothed,
      ci_external_pipelines_all_time_event,
      ci_external_pipelines_since_last_ping,
      FIRST_VALUE(ci_external_pipelines_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS ci_external_pipelines_per_day,
      (ci_external_pipelines_per_day * days_in_month_count)::INT                        AS ci_external_pipelines_smoothed,
      merge_requests_all_time_event,
      merge_requests_since_last_ping,
      FIRST_VALUE(merge_requests_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS merge_requests_per_day,
      (merge_requests_per_day * days_in_month_count)::INT                               AS merge_requests_smoothed,
      todos_all_time_event,
      todos_since_last_ping,
      FIRST_VALUE(todos_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS todos_per_day,
      (todos_per_day * days_in_month_count)::INT                                        AS todos_smoothed,
      epics_all_time_event,
      epics_since_last_ping,
      FIRST_VALUE(epics_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS epics_per_day,
      (epics_per_day * days_in_month_count)::INT                                        AS epics_smoothed,
      issues_all_time_event,
      issues_since_last_ping,
      FIRST_VALUE(issues_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS issues_per_day,
      (issues_per_day * days_in_month_count)::INT                                       AS issues_smoothed,
      projects_all_time_event,
      projects_since_last_ping,
      FIRST_VALUE(projects_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_per_day,
      (projects_per_day * days_in_month_count)::INT                                     AS projects_smoothed,
      sast_jobs_all_time_event,
      sast_jobs_since_last_ping,
      FIRST_VALUE(sast_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS sast_jobs_per_day,
      (sast_jobs_per_day * days_in_month_count)::INT                                    AS sast_jobs_smoothed,
      dast_jobs_all_time_event,
      dast_jobs_since_last_ping,
      FIRST_VALUE(dast_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS dast_jobs_per_day,
      (dast_jobs_per_day * days_in_month_count)::INT                                    AS dast_jobs_smoothed,
      dependency_scanning_jobs_all_time_event,
      dependency_scanning_jobs_since_last_ping,
      FIRST_VALUE(dependency_scanning_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS dependency_scanning_jobs_per_day,
      (dependency_scanning_jobs_per_day * days_in_month_count)::INT                     AS dependency_scanning_jobs_smoothed,
      license_management_jobs_all_time_event,
      license_management_jobs_since_last_ping,
      FIRST_VALUE(license_management_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS license_management_jobs_per_day,
      (license_management_jobs_per_day * days_in_month_count)::INT                      AS license_management_jobs_smoothed,
      secret_detection_jobs_all_time_event,
      secret_detection_jobs_since_last_ping,
      FIRST_VALUE(secret_detection_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS secret_detection_jobs_per_day,
      (secret_detection_jobs_per_day * days_in_month_count)::INT                        AS secret_detection_jobs_smoothed,
      container_scanning_jobs_all_time_event,
      container_scanning_jobs_since_last_ping,
      FIRST_VALUE(container_scanning_jobs_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS container_scanning_jobs_per_day,
      (container_scanning_jobs_per_day * days_in_month_count)::INT                      AS container_scanning_jobs_smoothed,
      projects_jenkins_active_all_time_event,
      projects_jenkins_active_since_last_ping,
      FIRST_VALUE(projects_jenkins_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_jenkins_active_per_day,
      (projects_jenkins_active_per_day * days_in_month_count)::INT                      AS projects_jenkins_active_smoothed,
      projects_bamboo_active_all_time_event,
      projects_bamboo_active_since_last_ping,
      FIRST_VALUE(projects_bamboo_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_bamboo_active_per_day,
      (projects_bamboo_active_per_day * days_in_month_count)::INT                       AS projects_bamboo_active_smoothed,
      projects_jira_active_all_time_event,
      projects_jira_active_since_last_ping,
      FIRST_VALUE(projects_jira_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_jira_active_per_day,
      (projects_jira_active_per_day * days_in_month_count)::INT                         AS projects_jira_active_smoothed,
      projects_drone_ci_active_all_time_event,
      projects_drone_ci_active_since_last_ping,
      FIRST_VALUE(projects_drone_ci_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_drone_ci_active_per_day,
      (projects_drone_ci_active_per_day * days_in_month_count)::INT                     AS projects_drone_ci_active_smoothed,
      projects_github_active_all_time_event,
      projects_github_active_since_last_ping,
      FIRST_VALUE(projects_github_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_github_active_per_day,
      (projects_github_active_per_day * days_in_month_count)::INT                       AS projects_github_active_smoothed,
      projects_jira_server_active_all_time_event,
      projects_jira_server_active_since_last_ping,
      FIRST_VALUE(projects_jira_server_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_jira_server_active_per_day,
      (projects_jira_server_active_per_day * days_in_month_count)::INT                  AS projects_jira_server_active_smoothed,
      projects_jira_dvcs_cloud_active_all_time_event,
      projects_jira_dvcs_cloud_active_since_last_ping,
      FIRST_VALUE(projects_jira_dvcs_cloud_active_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_jira_dvcs_cloud_active_per_day,
      (projects_jira_dvcs_cloud_active_per_day * days_in_month_count)::INT              AS projects_jira_dvcs_cloud_active_smoothed,
      projects_with_repositories_enabled_all_time_event,
      projects_with_repositories_enabled_since_last_ping,
      FIRST_VALUE(projects_with_repositories_enabled_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS projects_with_repositories_enabled_per_day,
      (projects_with_repositories_enabled_per_day * days_in_month_count)::INT           AS projects_with_repositories_enabled_smoothed,
      protected_branches_all_time_event,
      protected_branches_since_last_ping,
      FIRST_VALUE(protected_branches_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS protected_branches_per_day,
      (protected_branches_per_day * days_in_month_count)::INT                           AS protected_branches_smoothed,
      remote_mirrors_all_time_event,
      remote_mirrors_since_last_ping,
      FIRST_VALUE(remote_mirrors_since_last_ping / days_since_last_ping)
        IGNORE NULLS OVER (PARTITION BY dim_subscription_id ORDER BY snapshot_month
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)            AS remote_mirrors_per_day,
      (remote_mirrors_per_day * days_in_month_count)::INT                               AS remote_mirrors_smoothed
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
      commit_comment_all_time_event,
      commit_comment_since_last_ping,
      IFF(snapshot_month <= commit_comment_first_ping_month
            OR snapshot_month > commit_comment_last_ping_month,
          NULL, commit_comment_smoothed)                                                AS commit_comment_estimated_monthly,
      source_code_pushes_all_time_event,
      source_code_pushes_since_last_ping,
      IFF(snapshot_month <= source_code_pushes_first_ping_month
            OR snapshot_month > source_code_pushes_last_ping_month,
          NULL, source_code_pushes_smoothed)                                            AS source_code_pushes_estimated_monthly,
      ci_builds_all_time_event,
      ci_builds_since_last_ping,
      IFF(snapshot_month <= ci_builds_first_ping_month
            OR snapshot_month > ci_builds_last_ping_month,
          NULL, ci_builds_smoothed)                                                     AS ci_builds_estimated_monthly,
      ci_runners_all_time_event,
      ci_runners_since_last_ping,
      IFF(snapshot_month <= ci_runners_first_ping_month
            OR snapshot_month > ci_runners_last_ping_month,
          NULL, ci_runners_smoothed)                                                    AS ci_runners_estimated_monthly,
      template_repositories_all_time_event,
      template_repositories_since_last_ping,
      IFF(snapshot_month <= template_repositories_first_ping_month
            OR snapshot_month > template_repositories_last_ping_month,
          NULL, template_repositories_smoothed)                                         AS template_repositories_estimated_monthly,
      projects_with_packages_all_time_event,
      projects_with_packages_since_last_ping,
      IFF(snapshot_month <= projects_with_packages_first_ping_month
            OR snapshot_month > projects_with_packages_last_ping_month,
          NULL, projects_with_packages_smoothed)                                        AS projects_with_packages_estimated_monthly,
      auto_devops_enabled_all_time_event,
      auto_devops_enabled_since_last_ping,
      IFF(snapshot_month <= auto_devops_enabled_first_ping_month
            OR snapshot_month > auto_devops_enabled_last_ping_month,
          NULL, auto_devops_enabled_smoothed)                                           AS auto_devops_enabled_estimated_monthly,
      ci_builds_all_time_user,
      ci_build_users_since_last_ping,
      ci_internal_pipelines_all_time_event,
      ci_internal_pipelines_since_last_ping,
      IFF(snapshot_month <= ci_internal_pipelines_first_ping_month
            OR snapshot_month > ci_internal_pipelines_last_ping_month,
          NULL, ci_internal_pipelines_smoothed)                                         AS ci_internal_pipelines_estimated_monthly,
      ci_external_pipelines_all_time_event,
      ci_external_pipelines_since_last_ping,
      IFF(snapshot_month <= ci_external_pipelines_first_ping_month
            OR snapshot_month > ci_external_pipelines_last_ping_month,
          NULL, ci_external_pipelines_smoothed)                                         AS ci_external_pipelines_estimated_monthly,
      merge_requests_all_time_event,
      merge_requests_since_last_ping,
      IFF(snapshot_month <= merge_requests_first_ping_month
            OR snapshot_month > merge_requests_last_ping_month,
          NULL, merge_requests_smoothed)                                                AS merge_requests_estimated_monthly,
      todos_all_time_event,
      todos_since_last_ping,
      IFF(snapshot_month <= todos_first_ping_month
            OR snapshot_month > todos_last_ping_month,
          NULL, todos_smoothed)                                                         AS todos_estimated_monthly,
      epics_all_time_event,
      epics_since_last_ping,
      IFF(snapshot_month <= epics_first_ping_month
            OR snapshot_month > epics_last_ping_month,
          NULL, epics_smoothed)                                                         AS epics_estimated_monthly,
      issues_all_time_event,
      issues_since_last_ping,
      IFF(snapshot_month <= issues_first_ping_month
            OR snapshot_month > issues_last_ping_month,
          NULL, issues_smoothed)                                                        AS issues_estimated_monthly,
      projects_all_time_event,
      projects_since_last_ping,
      IFF(snapshot_month <= projects_first_ping_month
            OR snapshot_month > projects_last_ping_month,
          NULL, projects_smoothed)                                                      AS projects_estimated_monthly,
      sast_jobs_all_time_event,
      sast_jobs_since_last_ping,
      IFF(snapshot_month <= sast_jobs_first_ping_month
            OR snapshot_month > sast_jobs_last_ping_month,
          NULL, sast_jobs_smoothed)                                                     AS sast_jobs_estimated_monthly,
      dast_jobs_all_time_event,
      dast_jobs_since_last_ping,
      IFF(snapshot_month <= dast_jobs_first_ping_month
            OR snapshot_month > dast_jobs_last_ping_month,
          NULL, dast_jobs_smoothed)                                                     AS dast_jobs_estimated_monthly,
      dependency_scanning_jobs_all_time_event,
      dependency_scanning_jobs_since_last_ping,
      IFF(snapshot_month <= dependency_scanning_jobs_first_ping_month
            OR snapshot_month > dependency_scanning_jobs_last_ping_month,
          NULL, dependency_scanning_jobs_smoothed)                                      AS dependency_scanning_jobs_estimated_monthly,
      license_management_jobs_all_time_event,
      license_management_jobs_since_last_ping,
      IFF(snapshot_month <= license_management_jobs_first_ping_month
            OR snapshot_month > license_management_jobs_last_ping_month,
          NULL, license_management_jobs_smoothed)                                       AS license_management_jobs_estimated_monthly,
      secret_detection_jobs_all_time_event,
      secret_detection_jobs_since_last_ping,
      IFF(snapshot_month <= secret_detection_jobs_first_ping_month
            OR snapshot_month > secret_detection_jobs_last_ping_month,
          NULL, secret_detection_jobs_smoothed)                                         AS secret_detection_jobs_estimated_monthly,
      container_scanning_jobs_all_time_event,
      container_scanning_jobs_since_last_ping,
      IFF(snapshot_month <= container_scanning_jobs_first_ping_month
            OR snapshot_month > container_scanning_jobs_last_ping_month,
          NULL, container_scanning_jobs_smoothed)                                       AS container_scanning_jobs_estimated_monthly,
      projects_jenkins_active_all_time_event,
      projects_jenkins_active_since_last_ping,
      IFF(snapshot_month <= projects_jenkins_active_first_ping_month
            OR snapshot_month > projects_jenkins_active_last_ping_month,
          NULL, projects_jenkins_active_smoothed)                                       AS projects_jenkins_active_estimated_monthly,
      projects_bamboo_active_all_time_event,
      projects_bamboo_active_since_last_ping,
      IFF(snapshot_month <= projects_bamboo_active_first_ping_month
            OR snapshot_month > projects_bamboo_active_last_ping_month,
          NULL, projects_bamboo_active_smoothed)                                        AS projects_bamboo_active_estimated_monthly,
      projects_jira_active_all_time_event,
      projects_jira_active_since_last_ping,
      IFF(snapshot_month <= projects_jira_active_first_ping_month
            OR snapshot_month > projects_jira_active_last_ping_month,
          NULL, projects_jira_active_smoothed)                                          AS projects_jira_active_estimated_monthly,
      projects_drone_ci_active_all_time_event,
      projects_drone_ci_active_since_last_ping,
      IFF(snapshot_month <= projects_drone_ci_active_first_ping_month
            OR snapshot_month > projects_drone_ci_active_last_ping_month,
          NULL, projects_drone_ci_active_smoothed)                                      AS projects_drone_ci_active_estimated_monthly,
      projects_github_active_all_time_event,
      projects_github_active_since_last_ping,
      IFF(snapshot_month <= projects_github_active_first_ping_month
            OR snapshot_month > projects_github_active_last_ping_month,
          NULL, projects_github_active_smoothed)                                        AS projects_github_active_estimated_monthly,
      projects_jira_server_active_all_time_event,
      projects_jira_server_active_since_last_ping,
      IFF(snapshot_month <= projects_jira_server_active_first_ping_month
            OR snapshot_month > projects_jira_server_active_last_ping_month,
          NULL, projects_jira_server_active_smoothed)                                   AS projects_jira_server_active_estimated_monthly,
      projects_jira_dvcs_cloud_active_all_time_event,
      projects_jira_dvcs_cloud_active_since_last_ping,
      IFF(snapshot_month <= projects_jira_dvcs_cloud_active_first_ping_month
            OR snapshot_month > projects_jira_dvcs_cloud_active_last_ping_month,
          NULL, projects_jira_dvcs_cloud_active_smoothed)                               AS projects_jira_dvcs_cloud_active_estimated_monthly,
      projects_with_repositories_enabled_all_time_event,
      projects_with_repositories_enabled_since_last_ping,
      IFF(snapshot_month <= projects_with_repositories_enabled_first_ping_month
            OR snapshot_month > projects_with_repositories_enabled_last_ping_month,
          NULL, projects_with_repositories_enabled_smoothed)                            AS projects_with_repositories_enabled_estimated_monthly,
      protected_branches_all_time_event,
      protected_branches_since_last_ping,
      IFF(snapshot_month <= protected_branches_first_ping_month
            OR snapshot_month > protected_branches_last_ping_month,
          NULL, protected_branches_smoothed)                                            AS protected_branches_estimated_monthly,
      remote_mirrors_all_time_event,
      remote_mirrors_since_last_ping,
      IFF(snapshot_month <= remote_mirrors_first_ping_month
            OR snapshot_month > remote_mirrors_last_ping_month,
          NULL, remote_mirrors_smoothed)                                                AS remote_mirrors_estimated_monthly
    FROM smoothed_diffs
    LEFT JOIN ping_ranges
      ON smoothed_diffs.dim_subscription_id = ping_ranges.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-04",
    updated_date="2021-03-24"
) }}