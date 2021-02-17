WITH subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      first_day_of_month
    FROM {{ ref('bdg_subscription_product_rate_plan') }}
      INNER JOIN {{ ref('dim_date') }}
        ON date_actual BETWEEN '2017-04-01' AND DATE_TRUNC('month', CURRENT_DATE)
    WHERE product_delivery_type = 'Self-Managed'

), usage_ping AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_subscription_mapped_wave_2_3_metrics') }}
    WHERE ping_source = 'Self-Managed'
      AND dim_subscription_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        ping_created_at_month
      ORDER BY ping_created_at DESC
      ) = 1

), seat_link AS (

    SELECT *
    FROM {{ ref('fct_usage_self_managed_seat_link') }}

), joined AS (

    SELECT
      subscriptions.dim_subscription_id,
      subscriptions.dim_subscription_id_original,
      subscriptions.dim_billing_account_id,
      subscriptions.first_day_of_month                              AS snapshot_month,
      seat_link.report_date                                         AS seat_link_report_date,
      seat_link.active_user_count / seat_link.license_user_count    AS license_utilization,
      seat_link.active_user_count,
      seat_link.max_historical_user_count,
      seat_link.license_user_count,
      usage_ping.ping_created_at,
      usage_ping.uuid,
      usage_ping.hostname,
      usage_ping.dim_license_id,
      usage_ping.license_md5,
      usage_ping.cleaned_version,
      usage_ping.umau_28_days_user,
      usage_ping.action_monthly_active_users_project_repo_28_days_user,
      usage_ping.merge_requests_28_days_user,
      usage_ping.projects_with_repositories_enabled_28_days_user,
      usage_ping.commit_comment_all_time_event,
      usage_ping.source_code_pushes_all_time_event,
      usage_ping.ci_pipelines_28_days_user,
      usage_ping.ci_internal_pipelines_28_days_user,
      usage_ping.ci_builds_28_days_user,
      usage_ping.ci_runners_all_time_event,
      usage_ping.auto_devops_enable_all_time_event,
      usage_ping.gitlab_shared_runners_enabled_instance_setting,
      usage_ping.container_registry_enabled_instance_setting,
      usage_ping.template_repositories_all_time_event,
      usage_ping.ci_pipeline_config_repository_28_days_user,
      usage_ping.user_unique_users_all_secure_scanners_28_days_user,
      usage_ping.user_container_scanning_job_28_days_users,
      usage_ping.user_sast_jobs_28_days_user,
      usage_ping.user_dast_jobs_28_days_user,
      usage_ping.user_dependency_scanning_jobs_28_days_user,
      usage_ping.user_license_management_jobs_28_days_user,
      usage_ping.user_secret_detection_jobs_28_days_user,
      usage_ping.object_store_packages_enabled_instance_setting,
      usage_ping.projects_with_packages_all_time_event,
      usage_ping.projects_with_packages_28_days_user,
      usage_ping.deployments_28_days_user,
      usage_ping.releases_28_days_user,
      usage_ping.epics_28_days_user,
      usage_ping.issues_28_days_user,
      IFF(usage_ping.instance_user_count != seat_link.active_user_count,
          usage_ping.instance_user_count, NULL)                     AS instance_user_count_not_aligned,
      IFF(usage_ping.historical_max_users != seat_link.max_historical_user_count,
          usage_ping.historical_max_users, NULL)                    AS historical_max_users_not_aligned,
      seat_link.is_subscription_in_zuora                            AS is_seat_link_subscription_in_zuora,
      seat_link.is_rate_plan_in_zuora                               AS is_seat_link_rate_plan_in_zuora,
      seat_link.is_active_user_count_available                      AS is_seat_link_active_user_count_available,
      usage_ping.is_license_mapped_to_subscription                  AS is_usage_ping_license_mapped_to_subscription,
      usage_ping.is_license_subscription_id_valid                   AS is_usage_ping_license_subscription_id_valid,
      IFF(usage_ping.ping_created_at IS NOT NULL
          OR seat_link.report_date IS NOT NULL,
          TRUE, FALSE)                                              AS is_data_in_subscription_month
    FROM subscriptions
    LEFT JOIN usage_ping
      ON subscriptions.dim_subscription_id = usage_ping.dim_subscription_id
      AND subscriptions.first_day_of_month = usage_ping.ping_created_at_month
    LEFT JOIN seat_link
      ON subscriptions.dim_subscription_id = seat_link.dim_subscription_id
      AND subscriptions.first_day_of_month = seat_link.snapshot_month
  
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-08",
    updated_date="2021-02-08"
) }}
