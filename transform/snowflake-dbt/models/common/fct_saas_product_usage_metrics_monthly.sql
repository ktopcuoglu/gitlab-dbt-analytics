{{ simple_cte([
    ('saas_usage_ping', 'prep_saas_usage_ping_subscription_mapped_wave_2_3_metrics'),
    ('zuora_subscriptions', 'bdg_subscription_product_rate_plan'),
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('dates', 'dim_date')
]) }}

, saas_subscriptions AS (

    SELECT
      zuora_subscriptions.dim_subscription_id,
      zuora_subscriptions.dim_subscription_id_original,
      zuora_subscriptions.dim_billing_account_id,
      dates.first_day_of_month                                                          AS snapshot_month
    FROM zuora_subscriptions
    INNER JOIN dates
      ON dates.date_actual BETWEEN '2017-04-01' AND CURRENT_DATE                        -- first month Usage Ping was collected
    WHERE zuora_subscriptions.product_delivery_type = 'SaaS'
    {{ dbt_utils.group_by(n=4)}}

), gitlab_seats AS (
    
    SELECT
      gitlab_subscriptions.namespace_id,
      gitlab_subscriptions.seats,
      gitlab_subscriptions.seats_in_use,
      gitlab_subscriptions.max_seats_used,
      dates.first_day_of_month                                                          AS snapshot_month
    FROM gitlab_subscriptions
    INNER JOIN dates
      ON dates.date_actual BETWEEN gitlab_subscriptions.valid_from
                            AND IFNULL(gitlab_subscriptions.valid_to, CURRENT_DATE)
    {{ dbt_utils.group_by(n=5)}}

), joined AS (

    SELECT
      saas_subscriptions.dim_subscription_id,
      saas_subscriptions.dim_subscription_id_original,
      saas_subscriptions.dim_billing_account_id,
      saas_usage_ping.dim_namespace_id,
      saas_subscriptions.snapshot_month,
      {{ get_date_id('saas_subscriptions.snapshot_month') }}                            AS snapshot_date_id,
      saas_usage_ping.ping_date                                                         AS ping_created_at,
      {{ get_date_id('saas_usage_ping.ping_date') }}                                    AS ping_created_date_id,
      -- Wave 1
      gitlab_seats.seats                                                                AS subscription_seats,
      gitlab_seats.seats_in_use                                                         AS billable_user_count,
      DIV0(gitlab_seats.seats_in_use, gitlab_seats.seats)                               AS license_utilization,
      gitlab_seats.max_seats_used                                                       AS max_historical_user_count,
      -- Wave 2 & 3
      "usage_activity_by_stage_monthly.manage.events"                                   AS umau_28_days_user,
      "usage_activity_by_stage_monthly.create.action_monthly_active_users_project_repo" AS action_monthly_active_users_project_repo_28_days_user,
      "usage_activity_by_stage_monthly.create.merge_requests_users"                     AS merge_requests_28_days_user,
      "usage_activity_by_stage_monthly.create.projects_with_repositories_enabled"       AS projects_with_repositories_enabled_28_days_user,
      "counts.commit_comment"                                                           AS commit_comment_all_time_event,
      "counts.source_code_pushes"                                                       AS source_code_pushes_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipelines"                             AS ci_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_internal_pipelines"                    AS ci_internal_pipelines_28_days_user,
      "usage_activity_by_stage_monthly.verify.ci_builds"                                AS ci_builds_28_days_user,
    --   "" AS ci_builds_all_time_user,
      "counts.ci_builds"                                                                AS ci_builds_all_time_event,
      "counts.ci_runners"                                                               AS ci_runners_all_time_event,
      "counts.auto_devops_enabled"                                                      AS auto_devops_enabled_all_time_event,
      "gitlab_shared_runners_enabled"                                                   AS gitlab_shared_runners_enabled,
      "container_registry_enabled"                                                      AS container_registry_enabled,
      "counts.template_repositories"                                                    AS template_repositories_all_time_event,
      "usage_activity_by_stage_monthly.verify.ci_pipeline_config_repository"            AS ci_pipeline_config_repository_28_days_user,
      "user_unique_users_all_secure_scanners"                                           AS user_unique_users_all_secure_scanners_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_container_scanning_jobs"             AS user_container_scanning_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_sast_jobs"                           AS user_sast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dast_jobs"                           AS user_dast_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_dependency_scanning_jobs"            AS user_dependency_scanning_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_license_management_jobs"             AS user_license_management_jobs_28_days_user,
      "usage_activity_by_stage_monthly.secure.user_secret_detection_jobs"               AS user_secret_detection_jobs_28_days_user,
      "object_store.packages.enabled"                                                   AS object_store_packages_enabled,
      "counts.projects_with_packages"                                                   AS projects_with_packages_all_time_event,
    --   "" AS projects_with_packages_28_days_user,
      "usage_activity_by_stage_monthly.release.deployments"                             AS deployments_28_days_user,
      "usage_activity_by_stage_monthly.release.releases"                                AS releases_28_days_user,
      "usage_activity_by_stage_monthly.plan.epics"                                      AS epics_28_days_user,
      "usage_activity_by_stage_monthly.plan.issues"                                     AS issues_28_days_user,
      -- Wave 3.1
      "counts.ci_internal_pipelines"                                                    AS ci_internal_pipelines_all_time_event,
      "counts.ci_external_pipelines"                                                    AS ci_external_pipelines_all_time_event,
      "counts.merge_requests"                                                           AS merge_requests_all_time_event,
      "counts.todos"                                                                    AS todos_all_time_event,
      "counts.epics"                                                                    AS epics_all_time_event,
      "counts.issues"                                                                   AS issues_all_time_event,
      "counts.projects"                                                                 AS projects_all_time_event,
      "counts_monthly.deployments"                                                      AS deployments_28_days_event,
      "counts_monthly.packages"                                                         AS packages_28_days_event,
      "counts.sast_jobs"                                                                AS sast_jobs_all_time_event,
      "counts.dast_jobs"                                                                AS dast_jobs_all_time_event,
      "counts.dependency_scanning_jobs"                                                 AS dependency_scanning_jobs_all_time_event,
      "counts.license_management_jobs"                                                  AS license_management_jobs_all_time_event,
      "counts.secret_detection_jobs"                                                    AS secret_detection_jobs_all_time_event,
      "counts.container_scanning_jobs"                                                  AS container_scanning_jobs_all_time_event,
      "counts.projects_jenkins_active"                                                  AS projects_jenkins_active_all_time_event,
      "counts.projects_bamboo_active"                                                   AS projects_bamboo_active_all_time_event,
      "counts.projects_jira_active"                                                     AS projects_jira_active_all_time_event,
      "counts.projects_drone_ci_active"                                                 AS projects_drone_ci_active_all_time_event,
      "issue_imports.jira"                                                              AS jira_imports_28_days_event,
      "counts.projects_github_active"                                                   AS projects_github_active_all_time_event,
      "counts.projects_jira_server_active"                                              AS projects_jira_server_active_all_time_event,
      "counts.projects_jira_dvcs_cloud_active"                                          AS projects_jira_dvcs_cloud_active_all_time_event,
      "counts.projects_with_repositories_enabled"                                       AS projects_with_repositories_enabled_all_time_event,
      "counts.protected_branches"                                                       AS protected_branches_all_time_event,
      "counts.remote_mirrors"                                                           AS remote_mirrors_all_time_event,
      "usage_activity_by_stage.create.projects_enforcing_code_owner_approval"           AS projects_enforcing_code_owner_approval_28_days_user,
      "usage_activity_by_stage_monthly.configure.project_clusters_enabled"              AS project_clusters_enabled_28_days_user,
    --   "analytics_total_unique_counts_monthly" AS analytics_total_unique_counts_monthly,
    --   "redis_hll_counters.issues_edit.issues_edit_total_unique_counts_monthly" AS issues_edit_total_unique_counts_monthly,
    --   "redis_hll_counters.user_packages.user_packages_total_unique_counts_monthly" AS user_packages_total_unique_counts_monthly,
    --   "redis_hll_counters.terraform.p_terraform_state_api_unique_users_monthly" AS terraform_state_api_unique_users_monthly,
    --   "redis_hll_counters.incident_management.incident_management_total_unique_counts_monthly" AS incident_management_total_unique_counts_monthly,
      -- Data Quality Flags
      IFF(license_utilization = 0
            AND active_user_count > 0,
          TRUE, FALSE)                                                                  AS is_missing_paid_seats,
      IFF(saas_usage_ping.reporting_month IS NOT NULL
            OR gitlab_seats.snapshot_month IS NOT NULL,
          TRUE, FALSE)                                                                  AS is_data_in_subscription_month
    FROM saas_subscriptions
    LEFT JOIN saas_usage_ping
      ON saas_subscriptions.dim_subscription_id = saas_usage_ping.dim_subscription_id
      AND saas_subscriptions.snapshot_month = saas_usage_ping.reporting_month
    LEFT JOIN gitlab_seats
      ON saas_usage_ping.dim_namespace_id = gitlab_seats.namespace_id
      AND saas_usage_ping.reporting_month = gitlab_seats.snapshot_month
  
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-02",
    updated_date="2021-06-02"
) }}