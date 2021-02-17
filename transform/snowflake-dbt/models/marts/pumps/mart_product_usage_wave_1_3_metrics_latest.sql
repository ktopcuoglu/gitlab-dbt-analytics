{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('latest_metrics','fct_product_usage_wave_1_3_metrics_latest'),
    ('billing_accounts','dim_billing_account'),
    ('crm_accounts','dim_crm_account')
]) }}

, joined AS (

    SELECT
      latest_metrics.dim_subscription_id,
      latest_metrics.dim_subscription_id_original,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}      AS dim_billing_account_id,
      {{ get_keyed_nulls('crm_accounts.crm_account_id') }}                  AS dim_crm_account_id,
      latest_metrics.seat_link_report_date,
      latest_metrics.license_utilization,
      latest_metrics.active_user_count,
      latest_metrics.max_historical_user_count,
      latest_metrics.license_user_count,
      latest_metrics.ping_created_at,
      latest_metrics.uuid,
      latest_metrics.hostname,
      latest_metrics.dim_license_id,
      latest_metrics.license_md5,
      latest_metrics.cleaned_version,
      latest_metrics.umau_28_days_user,
      latest_metrics.action_monthly_active_users_project_repo_28_days_user,
      latest_metrics.merge_requests_28_days_user,
      latest_metrics.projects_with_repositories_enabled_28_days_user,
      latest_metrics.commit_comment_all_time_event,
      latest_metrics.source_code_pushes_all_time_event,
      latest_metrics.ci_pipelines_28_days_user,
      latest_metrics.ci_internal_pipelines_28_days_user,
      latest_metrics.ci_builds_28_days_user,
      latest_metrics.ci_runners_all_time_event,
      latest_metrics.auto_devops_enable_all_time_event,
      latest_metrics.gitlab_shared_runners_enabled_instance_setting,
      latest_metrics.container_registry_enabled_instance_setting,
      latest_metrics.template_repositories_all_time_event,
      latest_metrics.ci_pipeline_config_repository_28_days_user,
      latest_metrics.user_unique_users_all_secure_scanners_28_days_user,
      latest_metrics.user_container_scanning_job_28_days_users,
      latest_metrics.user_sast_jobs_28_days_user,
      latest_metrics.user_dast_jobs_28_days_user,
      latest_metrics.user_dependency_scanning_jobs_28_days_user,
      latest_metrics.user_license_management_jobs_28_days_user,
      latest_metrics.user_secret_detection_jobs_28_days_user,
      latest_metrics.object_store_packages_enabled_instance_setting,
      latest_metrics.projects_with_packages_all_time_event,
      latest_metrics.projects_with_packages_28_days_user,
      latest_metrics.deployments_28_days_user,
      latest_metrics.releases_28_days_user,
      latest_metrics.epics_28_days_user,
      latest_metrics.issues_28_days_user,
      latest_metrics.instance_user_count_not_aligned,
      latest_metrics.historical_max_users_not_aligned,
      latest_metrics.is_seat_link_subscription_in_zuora,
      latest_metrics.is_seat_link_rate_plan_in_zuora,
      latest_metrics.is_seat_link_active_user_count_available,
      latest_metrics.is_usage_ping_license_mapped_to_subscription,
      latest_metrics.is_usage_ping_license_subscription_id_valid,
      latest_metrics.is_data_in_subscription_month
    FROM latest_metrics
    LEFT JOIN billing_accounts
      ON latest_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN crm_accounts
      ON billing_accounts.dim_crm_account_id = crm_accounts.crm_account_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-11",
    updated_date="2021-02-11"
) }}
