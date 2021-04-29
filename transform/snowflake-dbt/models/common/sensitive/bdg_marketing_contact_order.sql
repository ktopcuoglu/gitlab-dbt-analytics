{{ simple_cte([
    ('marketing_contact', 'dim_marketing_contact'),
    ('marketing_contact_role', 'bdg_marketing_contact_role'),
    ('namespace_lineage', 'prep_namespace'),
    ('gitlab_namespaces', 'gitlab_dotcom_namespaces_source'),
    ('usage_ping_subscription_smau', 'fct_usage_ping_subscription_mapped_smau'),
    ('product_usage_wave_1_3', 'fct_product_usage_wave_1_3_metrics_monthly')
]) }}

, saas_namespace_subscription AS (
    
    SELECT *
    FROM {{ref('bdg_namespace_order_subscription')}}
    WHERE is_subscription_active = TRUE
      OR dim_subscription_id IS NULL

), self_managed_namespace_subscription AS (
    
    SELECT *
    FROM {{ref('bdg_self_managed_order_subscription')}}
    WHERE is_subscription_active = TRUE
      OR dim_subscription_id IS NULL

), usage_ping_subscription_smau_aggregate AS (

    SELECT 
      dim_subscription_id,
      manage_analytics_total_unique_counts_monthly,
      plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      create_repo_writes,
      verify_ci_pipelines_users_28_days,
      package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      release_release_creation_users_28_days,
      configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      monitor_incident_management_activer_user_28_days,
      secure_secure_scanners_users_28_days,
      protect_container_scanning_jobs_users_28_days
    FROM usage_ping_subscription_smau
    WHERE snapshot_month = DATE_TRUNC(MONTH, CURRENT_DATE)

), product_usage_wave_1_3_aggregate AS (

    SELECT 
      dim_subscription_id,
      umau_28_days_user,
      action_monthly_active_users_project_repo_28_days_user,
      merge_requests_28_days_user,
      commit_comment_all_time_event,
      source_code_pushes_all_time_event,
      ci_pipelines_28_days_user,
      ci_internal_pipelines_28_days_user,
      ci_builds_28_days_user,
      ci_builds_all_time_user,
      ci_builds_all_time_event,
      ci_runners_all_time_event,
      auto_devops_enabled_all_time_event,
      template_repositories_all_time_event,
      ci_pipeline_config_repository_28_days_user,
      user_unique_users_all_secure_scanners_28_days_user,
      user_container_scanning_jobs_28_days_user,
      user_sast_jobs_28_days_user,
      user_dast_jobs_28_days_user,
      user_dependency_scanning_jobs_28_days_user,
      user_license_management_jobs_28_days_user,
      user_secret_detection_jobs_28_days_user,
      projects_with_packages_all_time_event,
      projects_with_packages_28_days_user,
      deployments_28_days_user,
      releases_28_days_user,
      epics_28_days_user,
      issues_28_days_user,
      instance_user_count_not_aligned,
      historical_max_users_not_aligned
    FROM product_usage_wave_1_3
    WHERE snapshot_month = DATE_TRUNC(MONTH, CURRENT_DATE)

), prep AS (

     SELECT   
      marketing_contact.dim_marketing_contact_id,
      marketing_contact_role.marketing_contact_role,
      marketing_contact.email_address, 
      COALESCE(marketing_contact_role.namespace_id, 
               saas_namespace.dim_namespace_id, 
               saas_customer.dim_namespace_id, 
               saas_billing_account.dim_namespace_id)                                         AS dim_namespace_id,
      gitlab_namespaces.namespace_path,
      CASE 
        WHEN namespace_lineage.namespace_type = 'Individual' 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_individual_namespace,
      CASE 
        WHEN namespace_lineage.namespace_type = 'Group' 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_group_namespace,
      marketing_contact_role.customer_db_customer_id                                          AS customer_id,
      marketing_contact_role.zuora_billing_account_id                                         AS dim_billing_account_id,
      CASE
        WHEN saas_namespace.dim_namespace_id IS NOT NULL
          THEN saas_namespace.dim_subscription_id
        WHEN saas_customer.dim_namespace_id IS NOT NULL
          THEN saas_customer.dim_subscription_id
        WHEN saas_billing_account.dim_namespace_id IS NOT NULL
          THEN saas_billing_account.dim_subscription_id
        WHEN self_managed_customer.customer_id IS NOT NULL
          THEN self_managed_customer.dim_subscription_id
        WHEN self_managed_billing_account.customer_id IS NOT NULL
          THEN self_managed_billing_account.dim_subscription_id
      END                                                                                     AS dim_subscription_id,
      CASE
        WHEN saas_namespace.dim_namespace_id IS NOT NULL
          THEN saas_namespace.subscription_start_date
        WHEN saas_customer.dim_namespace_id IS NOT NULL
          THEN saas_customer.subscription_start_date
        WHEN saas_billing_account.dim_namespace_id IS NOT NULL
          THEN saas_billing_account.subscription_start_date
        WHEN self_managed_customer.customer_id IS NOT NULL
          THEN self_managed_customer.subscription_start_date
        WHEN self_managed_billing_account.customer_id IS NOT NULL
          THEN self_managed_billing_account.subscription_start_date
      END                                                                                     AS subscription_start_date,
      CASE
        WHEN saas_namespace.dim_namespace_id IS NOT NULL
          THEN saas_namespace.subscription_end_date
        WHEN saas_customer.dim_namespace_id IS NOT NULL
          THEN saas_customer.subscription_end_date
        WHEN saas_billing_account.dim_namespace_id IS NOT NULL
          THEN saas_billing_account.subscription_end_date
        WHEN self_managed_customer.customer_id IS NOT NULL
          THEN self_managed_customer.subscription_end_date
        WHEN self_managed_billing_account.customer_id IS NOT NULL
          THEN self_managed_billing_account.subscription_end_date
      END                                                                                     AS subscription_end_date,
      CASE 
        WHEN marketing_contact_role.namespace_id IS NOT NULL 
          AND saas_namespace.product_tier_name_namespace is NULL
          THEN 'SaaS - Free' 
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Personal Namespace Owner'
                                                                , 'Group Namespace Owner'
                                                                , 'Group Namespace Member'
                                                              ) 
          THEN saas_namespace.product_tier_name_namespace
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Customer DB Owner'
                                                              ) 
          THEN saas_customer.product_tier_name_with_trial   
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Zuora Billing Contact'
                                                              ) 
          THEN saas_billing_account.product_tier_name_subscription     
      END                                                                                     AS saas_product_tier,
      CASE 
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Customer DB Owner'
                                                              ) 
          THEN self_managed_customer.product_tier_name_with_trial   
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Zuora Billing Contact'
                                                              ) 
          THEN self_managed_billing_account.product_tier_name_subscription     
      END                                                                                     AS self_managed_product_tier,
      CASE 
        WHEN saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate' 
          OR saas_customer.order_is_trial = TRUE 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_trial,    
      CURRENT_DATE - CAST(saas_namespace.saas_trial_expired_on AS DATE)                       AS days_since_saas_trial_ended,
      CASE 
        WHEN saas_customer.order_is_trial 
          THEN CAST(saas_customer.order_end_date AS DATE)
        WHEN saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate'
          THEN CAST(COALESCE(saas_namespace.saas_trial_expired_on, saas_namespace.order_end_date) AS DATE)
      END                                                                                     AS trial_end_date,
      CASE 
        WHEN trial_end_date IS NOT NULL AND CURRENT_DATE <= trial_end_date
          THEN trial_end_date - CURRENT_DATE
      END                                                                                     AS days_until_saas_trial_ends,
      CASE 
        WHEN saas_product_tier = 'SaaS - Free' 
          THEN 1
        ELSE 0
      END                                                                                     AS is_saas_free_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Bronze' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_bronze_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Premium' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_premium_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Ultimate' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_ultimate_tier,       
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Starter' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_starter_tier,
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Premium' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_premium_tier,
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Ultimate' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_ultimate_tier
    
    FROM marketing_contact_role 
    INNER JOIN marketing_contact 
      ON marketing_contact.dim_marketing_contact_id = marketing_contact_role.dim_marketing_contact_id
    LEFT JOIN saas_namespace_subscription saas_namespace 
      ON saas_namespace.dim_namespace_id = marketing_contact_role.namespace_id
    LEFT JOIN saas_namespace_subscription saas_customer 
      ON saas_customer.customer_id = marketing_contact_role.customer_db_customer_id
    LEFT JOIN saas_namespace_subscription saas_billing_account 
      ON saas_billing_account.dim_billing_account_id = marketing_contact_role.zuora_billing_account_id   
    LEFT JOIN self_managed_namespace_subscription self_managed_customer 
      ON self_managed_customer.customer_id = marketing_contact_role.customer_db_customer_id
    LEFT JOIN self_managed_namespace_subscription self_managed_billing_account 
      ON self_managed_billing_account.dim_billing_account_id = marketing_contact_role.zuora_billing_account_id   
    LEFT JOIN namespace_lineage 
      ON namespace_lineage.dim_namespace_id = COALESCE(marketing_contact_role.namespace_id,
                                                   saas_namespace.dim_namespace_id,
                                                   saas_customer.dim_namespace_id,
                                                   saas_billing_account.dim_namespace_id)
    LEFT JOIN gitlab_namespaces 
      ON gitlab_namespaces.namespace_id = namespace_lineage.dim_namespace_id
      
), final AS (

    SELECT 
      prep.*,
      usage_ping_subscription_smau_aggregate.manage_analytics_total_unique_counts_monthly                                         AS smau_manage_analytics_total_unique_counts_monthly,
      usage_ping_subscription_smau_aggregate.plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly          AS smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      usage_ping_subscription_smau_aggregate.create_repo_writes                                                                   AS smau_create_repo_writes,
      usage_ping_subscription_smau_aggregate.verify_ci_pipelines_users_28_days                                                    AS smau_verify_ci_pipelines_users_28_days,
      usage_ping_subscription_smau_aggregate.package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly   AS smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      usage_ping_subscription_smau_aggregate.release_release_creation_users_28_days                                               AS smau_release_release_creation_users_28_days,
      usage_ping_subscription_smau_aggregate.configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly    AS smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      usage_ping_subscription_smau_aggregate.monitor_incident_management_activer_user_28_days                                     AS smau_monitor_incident_management_activer_user_28_days,
      usage_ping_subscription_smau_aggregate.secure_secure_scanners_users_28_days                                                 AS smau_secure_secure_scanners_users_28_days,
      usage_ping_subscription_smau_aggregate.protect_container_scanning_jobs_users_28_days                                        AS smau_protect_container_scanning_jobs_users_28_days,
      product_usage_wave_1_3_aggregate.umau_28_days_user                                                                          AS usage_umau_28_days_user,
      product_usage_wave_1_3_aggregate.action_monthly_active_users_project_repo_28_days_user                                      AS usage_action_monthly_active_users_project_repo_28_days_user,
      product_usage_wave_1_3_aggregate.merge_requests_28_days_user                                                                AS usage_merge_requests_28_days_user,
      product_usage_wave_1_3_aggregate.commit_comment_all_time_event                                                              AS usage_commit_comment_all_time_event,
      product_usage_wave_1_3_aggregate.source_code_pushes_all_time_event                                                          AS usage_source_code_pushes_all_time_event,
      product_usage_wave_1_3_aggregate.ci_pipelines_28_days_user                                                                  AS usage_ci_pipelines_28_days_user,
      product_usage_wave_1_3_aggregate.ci_internal_pipelines_28_days_user                                                         AS usage_ci_internal_pipelines_28_days_user,
      product_usage_wave_1_3_aggregate.ci_builds_28_days_user                                                                     AS usage_ci_builds_28_days_user,
      product_usage_wave_1_3_aggregate.ci_builds_all_time_user                                                                    AS usage_ci_builds_all_time_user,
      product_usage_wave_1_3_aggregate.ci_builds_all_time_event                                                                   AS usage_ci_builds_all_time_event,
      product_usage_wave_1_3_aggregate.ci_runners_all_time_event                                                                  AS usage_ci_runners_all_time_event,
      product_usage_wave_1_3_aggregate.auto_devops_enabled_all_time_event                                                         AS usage_auto_devops_enabled_all_time_event,
      product_usage_wave_1_3_aggregate.template_repositories_all_time_event                                                       AS usage_template_repositories_all_time_event,
      product_usage_wave_1_3_aggregate.ci_pipeline_config_repository_28_days_user                                                 AS usage_ci_pipeline_config_repository_28_days_user,
      product_usage_wave_1_3_aggregate.user_unique_users_all_secure_scanners_28_days_user                                         AS usage_user_unique_users_all_secure_scanners_28_days_user,
      product_usage_wave_1_3_aggregate.user_container_scanning_jobs_28_days_user                                                  AS usage_user_container_scanning_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.user_sast_jobs_28_days_user                                                                AS usage_user_sast_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.user_dast_jobs_28_days_user                                                                AS usage_user_dast_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.user_dependency_scanning_jobs_28_days_user                                                 AS usage_user_dependency_scanning_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.user_license_management_jobs_28_days_user                                                  AS usage_user_license_management_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.user_secret_detection_jobs_28_days_user                                                    AS usage_user_secret_detection_jobs_28_days_user,
      product_usage_wave_1_3_aggregate.projects_with_packages_all_time_event                                                      AS usage_projects_with_packages_all_time_event,
      product_usage_wave_1_3_aggregate.projects_with_packages_28_days_user                                                        AS usage_projects_with_packages_28_days_user,
      product_usage_wave_1_3_aggregate.deployments_28_days_user                                                                   AS usage_deployments_28_days_user,
      product_usage_wave_1_3_aggregate.releases_28_days_user                                                                      AS usage_releases_28_days_user,
      product_usage_wave_1_3_aggregate.epics_28_days_user                                                                         AS usage_epics_28_days_user,
      product_usage_wave_1_3_aggregate.issues_28_days_user                                                                        AS usage_issues_28_days_user,
      product_usage_wave_1_3_aggregate.instance_user_count_not_aligned                                                            AS usage_instance_user_count_not_aligned,
      product_usage_wave_1_3_aggregate.historical_max_users_not_aligned                                                           AS usage_historical_max_users_not_aligned
    FROM prep
    LEFT JOIN usage_ping_subscription_smau_aggregate
      ON usage_ping_subscription_smau_aggregate.dim_subscription_id = prep.dim_subscription_id
    LEFT JOIN product_usage_wave_1_3_aggregate
      ON product_usage_wave_1_3_aggregate.dim_subscription_id = prep.dim_subscription_id

)
    

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@ischweickartDD",
    created_date="2021-02-04",
    updated_date="2021-04-06"
) }}
