{{ simple_cte([
    ('subscriptions', 'bdg_subscription_product_rate_plan'),
    ('dates', 'dim_date'),
    ('gmau_metrics','prep_usage_ping_subscription_mapped_gmau')
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

), gmau_monthly AS (

    SELECT *
    FROM gmau_metrics
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
      gmau_monthly.dim_crm_account_id,
      gmau_monthly.dim_parent_crm_account_id,
      gmau_monthly.dim_usage_ping_id,
      gmau_monthly.uuid,
      gmau_monthly.hostname,
      gmau_monthly.dim_license_id,
      gmau_monthly.license_md5,
      gmau_monthly.cleaned_version,
      gmau_monthly.ping_created_at,
      {{ get_date_id('gmau_monthly.ping_created_at') }}             AS ping_created_date_id,
      /* Missing: */
        --> Manage:Access (user_auth_by_provider)
        --> Enablement:Infrastructure (COUNT_EVENTS_ACTIVE_USERS_LAST_28_DAYS_BY_PLAN_WAS_PAID)
        --> Package:Package (user_packages_total_unique_counts_monthly)
        --> Secure:Threat Insights ()
      /* Different metric definition: */
        --> Enablement:Geo (geo_nodes)
        --> Release:Release (deployments)
        --> Secure:Fuzz Testing ()
      /* Manage:Optimize */gmau_monthly.analytics_analytics_total_unique_counts_monthly,
      /* Manage:Compliance */gmau_monthly.compliance_compliance_total_unique_counts_monthly,
      /* */gmau_monthly.knowledge_action_monthly_active_users_design_management,
      /* Manage:Import */gmau_monthly.import_usage_activity_by_stage_monthly_manage_projects_imported_total,
      /* Plan:Product Planning */gmau_monthly.portfolio_management_epic_creation_users_28_days,
      /* Plan:Project Management */gmau_monthly.project_management_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      /* Create:Source Code */gmau_monthly.source_code_repo_writes,
      /* Create:Editor  */gmau_monthly.editor_ide_edit_users_28_days,
      /*  */gmau_monthly.static_site_editor_static_site_editor_views_28_days,
      /* Create:Ecosystem */gmau_monthly.ecosystem_redis_hll_counters_ecosystem_ecosystem_total_unique_counts_monthly,
      /* Enablement:Geo */gmau_monthly.geo_usage_activity_by_stage_monthly_enablement_usage_activity_by_stage_monthly_manage_groups,
      /* Enablement:Global Search(?) */gmau_monthly.global_search_paid_search_28_days
                                     + gmau_monthly.global_search_search_users_28_days,
      /* Verify:CI */gmau_monthly.continuous_integration_ci_pipelines_users_28_days,
      /* Verify:Pipeline Authoring */gmau_monthly.code_review_merge_request_interaction_users_28_days,
      /* Verify:Testing */gmau_monthly.testing_counts_monthly_aggregated_metrics_i_testing_paid_monthly_active_user_total,
      /* Release:Release */gmau_monthly.release_management_release_creation_users_28_days,
      /* Configure:Configure */gmau_monthly.configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      /* Monitor:Monitor */gmau_monthly.monitor_incident_management_activer_user_28_days,
      /* Secure:Static Analysis */CASE
                                    WHEN gmau_monthly.static_analysis_static_analysis_jobs_users_28_days > gmau_monthly.static_analysis_static_analysis_jobs_users_28_days(1)
                                    THEN gmau_monthly.static_analysis_static_analysis_jobs_users_28_days
                                    ELSE gmau_monthly.static_analysis_static_analysis_jobs_users_28_days(1)
                                  END AS static_analysis_static_analysis_jobs_users_28_days,
      /* Secure:Dynamic Analysis */gmau_monthly.dynamic_analysis_dast_jobs_users_28_days,
      /* Secure:Composition Analysis */CASE 
                                        WHEN gmau_monthly.composition_analysis_dependency_scanning_jobs_users_28_days > gmau_monthly.composition_analysis_license_management_jobs_user_28_days
                                        THEN gmau_monthly.composition_analysis_dependency_scanning_jobs_users_28_days
                                        ELSE gmau_monthly.composition_analysis_license_management_jobs_user_28_days
                                       END AS [Secure:Composition Analysis](?),
      /*  */gmau_monthly.composition_analysis_license_scanning_jobs_users_28_days,
      /* Secure:Fuzz Testing */gmau_monthly.fuzz_testing_fuzz_testing_jobs_users_28_days,
      /* Protect:Container Security */gmau_monthly.container_security_container_scanning_jobs_users_28_days,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY gmau_monthly.dim_subscription_id
            ORDER BY gmau_monthly.ping_created_at DESC) = 1,
          TRUE, FALSE)                                              AS is_latest_gmau_reported
    FROM sm_subscriptions
    LEFT JOIN gmau_monthly
      ON sm_subscriptions.dim_subscription_id = gmau_monthly.dim_subscription_id
      AND sm_subscriptions.snapshot_month = gmau_monthly.ping_created_at_month

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-15",
    updated_date="2021-03-15"
) }}