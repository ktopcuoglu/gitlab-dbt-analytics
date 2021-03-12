{{ simple_cte([
    ('subscriptions', 'bdg_subscription_product_rate_plan'),
    ('dates', 'dim_date'),
    ('smau_metrics','prep_usage_ping_subscription_mapped_smau')
]) }}

, sm_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      dim_subscription_id_original,
      dim_billing_account_id,
      first_day_of_month                                            AS snapshot_month
    FROM subscriptions
    INNER JOIN dates
      ON date_actual BETWEEN '2017-04-01' AND DATE_TRUNC('month', CURRENT_DATE) -- first month Usage Ping was collected
    WHERE product_delivery_type = 'Self-Managed'

), smau_monthly AS (

    SELECT *
    FROM smau_metrics
    WHERE dim_subscription_id IS NOT NULL
      AND ping_source = 'Self-Managed'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        ping_created_at_month
      ORDER BY ping_created_at DESC
      ) = 1

), joined AS (

    SELECT 
      sm_subscriptions.dim_subscription_id,
      sm_subscriptions.snapshot_month,
      {{ get_date_id('sm_subscriptions.snapshot_month') }}          AS snapshot_date_id,
      sm_subscriptions.dim_subscription_id_original,
      sm_subscriptions.dim_billing_account_id,
      smau_monthly.dim_crm_account_id,
      smau_monthly.dim_parent_crm_account_id,
      smau_monthly.dim_usage_ping_id,
      smau_monthly.uuid,
      smau_monthly.hostname,
      smau_monthly.dim_license_id,
      smau_monthly.license_md5,
      smau_monthly.cleaned_version,
      smau_monthly.ping_created_at,
      {{ get_date_id('smau_monthly.ping_created_at') }}             AS ping_created_date_id,
      smau_monthly.manage_analytics_total_unique_counts_monthly,
      smau_monthly.plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      smau_monthly.create_repo_writes,
      smau_monthly.verify_ci_pipelines_users_28_days,
      smau_monthly.package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      smau_monthly.release_release_creation_users_28_days,
      smau_monthly.configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      smau_monthly.monitor_incident_management_activer_user_28_days,
      smau_monthly.secure_secure_scanners_users_28_days,
      smau_monthly.protect_container_scanning_jobs_users_28_days,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY smau_monthly.dim_subscription_id
            ORDER BY smau_monthly.ping_created_at DESC) = 1,
          TRUE, FALSE)                                              AS is_latest_smau_reported
    FROM sm_subscriptions
    LEFT JOIN smau_monthly
      ON sm_subscriptions.dim_subscription_id = smau_monthly.dim_subscription_id
      AND sm_subscriptions.snapshot_month = smau_monthly.ping_created_at_month

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-15",
    updated_date="2021-03-15"
) }}