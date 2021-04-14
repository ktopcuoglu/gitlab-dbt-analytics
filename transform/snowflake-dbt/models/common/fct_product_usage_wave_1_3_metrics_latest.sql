WITH subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id
    FROM {{ ref('bdg_subscription_product_rate_plan') }}
    WHERE product_delivery_type = 'Self-Managed'

), usage_ping AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_subscription_mapped_wave_2_3_metrics') }}
    WHERE ping_source = 'Self-Managed'
      AND dim_subscription_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_subscription_id
      ORDER BY ping_created_at DESC
      ) = 1

), seat_link AS (

    SELECT *
    FROM {{ ref('fct_usage_self_managed_seat_link') }}
    WHERE is_last_seat_link_report_per_subscription = TRUE

), smau AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_subscription_mapped_smau') }}
    WHERE is_latest_smau_reported = TRUE

), joined AS (

    SELECT
      subscriptions.dim_subscription_id,
      subscriptions.dim_subscription_id_original,
      subscriptions.dim_billing_account_id,
      seat_link.report_date                                                                   AS seat_link_report_date,
      {{ get_date_id('seat_link.report_date') }}                                              AS seat_link_report_date_id,
      usage_ping.dim_usage_ping_id,
      usage_ping.ping_created_at,
      {{ get_date_id('usage_ping.ping_created_at') }}                                         AS ping_created_date_id,
      usage_ping.uuid,
      usage_ping.hostname,
      usage_ping.instance_type,
      usage_ping.dim_license_id,
      usage_ping.license_md5,
      usage_ping.cleaned_version,
      -- Wave 1
      seat_link.active_user_count / seat_link.license_user_count                              AS license_utilization,
      seat_link.active_user_count,
      seat_link.max_historical_user_count,
      seat_link.license_user_count,
      -- Wave 2 & 3
      usage_ping.umau_28_days_user,
      usage_ping.action_monthly_active_users_project_repo_28_days_user,
      usage_ping.merge_requests_28_days_user,
      usage_ping.projects_with_repositories_enabled_28_days_user,
      usage_ping.commit_comment_all_time_event,
      usage_ping.source_code_pushes_all_time_event,
      usage_ping.ci_pipelines_28_days_user,
      usage_ping.ci_internal_pipelines_28_days_user,
      usage_ping.ci_builds_28_days_user,
      usage_ping.ci_builds_all_time_user,
      usage_ping.ci_builds_all_time_event,
      usage_ping.ci_runners_all_time_event,
      usage_ping.auto_devops_enabled_all_time_event,
      usage_ping.instance_gitlab_shared_runners_enabled,
      usage_ping.instance_container_registry_enabled,
      usage_ping.template_repositories_all_time_event,
      usage_ping.ci_pipeline_config_repository_28_days_user,
      usage_ping.user_unique_users_all_secure_scanners_28_days_user,
      usage_ping.user_sast_jobs_28_days_user,
      usage_ping.user_dast_jobs_28_days_user,
      usage_ping.user_dependency_scanning_jobs_28_days_user,
      usage_ping.user_license_management_jobs_28_days_user,
      usage_ping.user_secret_detection_jobs_28_days_user,
      usage_ping.user_container_scanning_jobs_28_days_user,
      usage_ping.instance_object_store_packages_enabled,
      usage_ping.projects_with_packages_all_time_event,
      usage_ping.projects_with_packages_28_days_user,
      usage_ping.deployments_28_days_user,
      usage_ping.releases_28_days_user,
      usage_ping.epics_28_days_user,
      usage_ping.issues_28_days_user,
      -- Wave 3.1
      usage_ping.ci_internal_pipelines_all_time_event,
      usage_ping.ci_external_pipelines_all_time_event,
      usage_ping.merge_requests_all_time_event,
      usage_ping.todos_all_time_event,
      usage_ping.epics_all_time_event,
      usage_ping.issues_all_time_event,
      usage_ping.projects_all_time_event,
      usage_ping.deployments_28_days_event,
      usage_ping.packages_28_days_event,
      usage_ping.sast_jobs_all_time_event,
      usage_ping.dast_jobs_all_time_event,
      usage_ping.dependency_scanning_jobs_all_time_event,
      usage_ping.license_management_jobs_all_time_event,
      usage_ping.secret_detection_jobs_all_time_event,
      usage_ping.container_scanning_jobs_all_time_event,
      usage_ping.projects_jenkins_active_all_time_event,
      usage_ping.projects_bamboo_active_all_time_event,
      usage_ping.projects_jira_active_all_time_event,
      usage_ping.projects_drone_ci_active_all_time_event,
      usage_ping.jira_imports_28_days_event,
      usage_ping.projects_github_active_all_time_event,
      usage_ping.projects_jira_server_active_all_time_event,
      usage_ping.projects_jira_dvcs_cloud_active_all_time_event,
      usage_ping.projects_with_repositories_enabled_all_time_event,
      usage_ping.protected_branches_all_time_event,
      usage_ping.remote_mirrors_all_time_event,
      usage_ping.projects_enforcing_code_owner_approval_28_days_user,
      usage_ping.project_clusters_enabled_28_days_user,
      smau.manage_analytics_total_unique_counts_monthly                                       AS analytics_28_days_user,
      smau.plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly        AS issues_edit_28_days_user,
      smau.package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly AS user_packages_28_days_user,
      smau.configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly  AS terraform_state_api_28_days_user,
      smau.monitor_incident_management_activer_user_28_days                                   AS incident_management_28_days_user,
      -- Wave 3.2
      usage_ping.instance_auto_devops_enabled,
      usage_ping.instance_gitaly_clusters,
      usage_ping.instance_epics_deepest_relationship_level,
      usage_ping.clusters_applications_cilium_all_time_event,
      usage_ping.network_policy_forwards_all_time_event,
      usage_ping.network_policy_drops_all_time_event,
      usage_ping.requirements_with_test_report_all_time_event,
      usage_ping.requirement_test_reports_ci_all_time_event,
      usage_ping.projects_imported_from_github_all_time_event,
      usage_ping.projects_jira_cloud_active_all_time_event,
      usage_ping.projects_jira_dvcs_server_active_all_time_event,
      usage_ping.service_desk_issues_all_time_event,
      usage_ping.ci_pipelines_all_time_user,
      usage_ping.service_desk_issues_28_days_user,
      usage_ping.projects_jira_active_28_days_user,
      usage_ping.projects_jira_dvcs_cloud_active_28_days_user,
      usage_ping.projects_jira_dvcs_server_active_28_days_user,
      usage_ping.merge_requests_with_required_code_owners_28_days_user,
      usage_ping.analytics_value_stream_28_days_event,
      usage_ping.code_review_user_approve_mr_28_days_user,
      usage_ping.epics_usage_28_days_user,
      usage_ping.ci_templates_usage_28_days_event,
      usage_ping.project_management_issue_milestone_changed_28_days_user,
      usage_ping.project_management_issue_iteration_changed_28_days_user,
      -- Data Quality Flags
      IFF(usage_ping.instance_user_count != seat_link.active_user_count,
          usage_ping.instance_user_count, NULL)                                               AS instance_user_count_not_aligned,
      IFF(usage_ping.historical_max_users != seat_link.max_historical_user_count,
          usage_ping.historical_max_users, NULL)                                              AS historical_max_users_not_aligned,
      seat_link.is_subscription_in_zuora                                                      AS is_seat_link_subscription_in_zuora,
      seat_link.is_rate_plan_in_zuora                                                         AS is_seat_link_rate_plan_in_zuora,
      seat_link.is_active_user_count_available                                                AS is_seat_link_active_user_count_available,
      usage_ping.is_license_mapped_to_subscription                                            AS is_usage_ping_license_mapped_to_subscription,
      usage_ping.is_license_subscription_id_valid                                             AS is_usage_ping_license_subscription_id_valid,
      IFF(usage_ping.ping_created_at IS NOT NULL
          OR seat_link.report_date IS NOT NULL,
          TRUE, FALSE)                                                                        AS is_data_in_subscription_month
    FROM subscriptions
    LEFT JOIN seat_link
      ON subscriptions.dim_subscription_id = seat_link.dim_subscription_id
    LEFT JOIN usage_ping
      ON subscriptions.dim_subscription_id = usage_ping.dim_subscription_id
    LEFT JOIN smau
      ON subscriptions.dim_subscription_id = smau.dim_subscription_id
  
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-08",
    updated_date="2021-04-13"
) }}