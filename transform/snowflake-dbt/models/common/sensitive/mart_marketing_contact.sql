WITH marketing_contact AS (

    SELECT * 
    FROM {{ref('dim_marketing_contact')}}

), marketing_contact_order AS (
  
    SELECT * 
    FROM {{ref('bdg_marketing_contact_order')}}

), subscription_aggregate AS (

    SELECT 
      dim_marketing_contact_id,
      MIN(subscription_start_date)                                                               AS min_subscription_start_date,
      MAX(subscription_end_date)                                                                 AS max_subscription_end_date
    FROM marketing_contact_order
    WHERE subscription_start_date is not null
    GROUP BY dim_marketing_contact_id

), paid_subscription_aggregate AS (

    SELECT 
      dim_marketing_contact_id,
      COUNT(DISTINCT dim_subscription_id)                                                        AS nbr_of_paid_subscriptions
    FROM marketing_contact_order
    WHERE dim_subscription_id is not null
      AND (is_saas_bronze_tier 
           OR is_saas_premium_tier 
           OR is_saas_ultimate_tier 
           OR is_self_managed_starter_tier
           OR is_self_managed_premium_tier
           OR is_self_managed_ultimate_tier
          )
    GROUP BY dim_marketing_contact_id

), distinct_contact_subscription AS (

    SELECT DISTINCT
      dim_marketing_contact_id,
      dim_subscription_id,
      smau_manage_analytics_total_unique_counts_monthly,
      smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      smau_create_repo_writes,
      smau_verify_ci_pipelines_users_28_days,
      smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      smau_release_release_creation_users_28_days,
      smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      smau_monitor_incident_management_activer_user_28_days,
      smau_secure_secure_scanners_users_28_days,
      smau_protect_container_scanning_jobs_users_28_days,
      usage_umau_28_days_user,
      usage_action_monthly_active_users_project_repo_28_days_user,
      usage_merge_requests_28_days_user,
      usage_commit_comment_all_time_event,
      usage_source_code_pushes_all_time_event,
      usage_ci_pipelines_28_days_user,
      usage_ci_internal_pipelines_28_days_user,
      usage_ci_builds_28_days_user,
      usage_ci_builds_all_time_user,
      usage_ci_builds_all_time_event,
      usage_ci_runners_all_time_event,
      usage_auto_devops_enabled_all_time_event,
      usage_template_repositories_all_time_event,
      usage_ci_pipeline_config_repository_28_days_user,
      usage_user_unique_users_all_secure_scanners_28_days_user,
      usage_user_container_scanning_jobs_28_days_user,
      usage_user_sast_jobs_28_days_user,
      usage_user_dast_jobs_28_days_user,
      usage_user_dependency_scanning_jobs_28_days_user,
      usage_user_license_management_jobs_28_days_user,
      usage_user_secret_detection_jobs_28_days_user,
      usage_projects_with_packages_all_time_event,
      usage_projects_with_packages_28_days_user,
      usage_deployments_28_days_user,
      usage_releases_28_days_user,
      usage_epics_28_days_user,
      usage_issues_28_days_user,
      usage_instance_user_count_not_aligned,
      usage_historical_max_users_not_aligned
    FROM marketing_contact_order
    WHERE dim_subscription_id IS NOT NULL

), usage_metrics AS (

    SELECT 
      dim_marketing_contact_id,
      SUM(smau_manage_analytics_total_unique_counts_monthly)                                        AS smau_manage_analytics_total_unique_counts_monthly,
      SUM(smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly)         AS smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      SUM(smau_create_repo_writes)                                                                  AS smau_create_repo_writes,
      SUM(smau_verify_ci_pipelines_users_28_days)                                                   AS smau_verify_ci_pipelines_users_28_days,
      SUM(smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly)  AS smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      SUM(smau_release_release_creation_users_28_days)                                              AS smau_release_release_creation_users_28_days,
      SUM(smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly)   AS smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      SUM(smau_monitor_incident_management_activer_user_28_days)                                    AS smau_monitor_incident_management_activer_user_28_days,
      SUM(smau_secure_secure_scanners_users_28_days)                                                AS smau_secure_secure_scanners_users_28_days,
      SUM(smau_protect_container_scanning_jobs_users_28_days)                                       AS smau_protect_container_scanning_jobs_users_28_days,
      SUM(usage_umau_28_days_user)                                                                  AS usage_umau_28_days_user,
      SUM(usage_action_monthly_active_users_project_repo_28_days_user)                              AS usage_action_monthly_active_users_project_repo_28_days_user,
      SUM(usage_merge_requests_28_days_user)                                                        AS usage_merge_requests_28_days_user,
      SUM(usage_commit_comment_all_time_event)                                                      AS usage_commit_comment_all_time_event,
      SUM(usage_source_code_pushes_all_time_event)                                                  AS usage_source_code_pushes_all_time_event,
      SUM(usage_ci_pipelines_28_days_user)                                                          AS usage_ci_pipelines_28_days_user,
      SUM(usage_ci_internal_pipelines_28_days_user)                                                 AS usage_ci_internal_pipelines_28_days_user,
      SUM(usage_ci_builds_28_days_user)                                                             AS usage_ci_builds_28_days_user,
      SUM(usage_ci_builds_all_time_user)                                                            AS usage_ci_builds_all_time_user,
      SUM(usage_ci_builds_all_time_event)                                                           AS usage_ci_builds_all_time_event,
      SUM(usage_ci_runners_all_time_event)                                                          AS usage_ci_runners_all_time_event,
      SUM(usage_auto_devops_enabled_all_time_event)                                                 AS usage_auto_devops_enabled_all_time_event,
      SUM(usage_template_repositories_all_time_event)                                               AS usage_template_repositories_all_time_event,
      SUM(usage_ci_pipeline_config_repository_28_days_user)                                         AS usage_ci_pipeline_config_repository_28_days_user,
      SUM(usage_user_unique_users_all_secure_scanners_28_days_user)                                 AS usage_user_unique_users_all_secure_scanners_28_days_user,
      SUM(usage_user_container_scanning_jobs_28_days_user)                                          AS usage_user_container_scanning_jobs_28_days_user,
      SUM(usage_user_sast_jobs_28_days_user)                                                        AS usage_user_sast_jobs_28_days_user,
      SUM(usage_user_dast_jobs_28_days_user)                                                        AS usage_user_dast_jobs_28_days_user,
      SUM(usage_user_dependency_scanning_jobs_28_days_user)                                         AS usage_user_dependency_scanning_jobs_28_days_user,
      SUM(usage_user_license_management_jobs_28_days_user)                                          AS usage_user_license_management_jobs_28_days_user,
      SUM(usage_user_secret_detection_jobs_28_days_user)                                            AS usage_user_secret_detection_jobs_28_days_user,
      SUM(usage_projects_with_packages_all_time_event)                                              AS usage_projects_with_packages_all_time_event,
      SUM(usage_projects_with_packages_28_days_user)                                                AS usage_projects_with_packages_28_days_user,
      SUM(usage_deployments_28_days_user)                                                           AS usage_deployments_28_days_user,
      SUM(usage_releases_28_days_user)                                                              AS usage_releases_28_days_user,
      SUM(usage_epics_28_days_user)                                                                 AS usage_epics_28_days_user,
      SUM(usage_issues_28_days_user)                                                                AS usage_issues_28_days_user,
      SUM(usage_instance_user_count_not_aligned)                                                    AS usage_instance_user_count_not_aligned,
      SUM(usage_historical_max_users_not_aligned)                                                   AS usage_historical_max_users_not_aligned
    FROM distinct_contact_subscription
    GROUP BY dim_marketing_contact_id

), prep AS (
  
    SELECT     
      marketing_contact.dim_marketing_contact_id,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Member' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_member,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Personal Namespace Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_individual_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Customer DB Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_customer_db_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Zuora Billing Contact' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_zuora_billing_contact,
      MIN(marketing_contact_order.days_since_saas_trial_ended)                                   AS days_since_saas_trial_ended,
      MAX(marketing_contact_order.days_until_saas_trial_ends)                                    AS days_until_saas_trial_ends,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN marketing_contact_order.is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_trial,   
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN marketing_contact_order.is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN marketing_contact_order.is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN marketing_contact_order.is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN marketing_contact_order.is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_ultimate_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_trial,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_ultimate_tier,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         ) 
                    THEN marketing_contact_order.is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_trial,    
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0 
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_ultimate_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         ) 
                    THEN marketing_contact_order.is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_trial,    
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0 
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_ultimate_tier,      
      CASE 
        WHEN MAX(marketing_contact_order.is_self_managed_starter_tier)  >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_starter_tier, 
      CASE 
        WHEN MAX(marketing_contact_order.is_self_managed_premium_tier)  >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_premium_tier, 
      CASE 
        WHEN MAX(marketing_contact_order.is_self_managed_ultimate_tier) >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_ultimate_tier,
      ARRAY_AGG(
                DISTINCT IFNULL(marketing_contact_order.marketing_contact_role || ': ' || 
                  IFNULL(marketing_contact_order.saas_product_tier, '') || IFNULL(marketing_contact_order.self_managed_product_tier, ''), 'No Role') 
               )                                                                                 AS role_tier_text,
      ARRAY_AGG(
                DISTINCT IFNULL(marketing_contact_order.marketing_contact_role || ': ' || 
                  IFNULL(marketing_contact_order.namespace_path, CASE 
                                          WHEN marketing_contact_order.self_managed_product_tier IS NOT NULL
                                            THEN 'Self-Managed' 
                                          ELSE '' 
                                        END)  || ' | ' || 
                  IFNULL(marketing_contact_order.saas_product_tier, '') || 
                  IFNULL(marketing_contact_order.self_managed_product_tier, ''), 'No Namespace')
               )                                                                                 AS role_tier_namespace_text

    FROM marketing_contact
    LEFT JOIN  marketing_contact_order
      ON marketing_contact_order.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    GROUP BY marketing_contact.dim_marketing_contact_id

), joined AS (

    SELECT 
      prep.*, 
      subscription_aggregate.min_subscription_start_date,
      subscription_aggregate.max_subscription_end_date,
      paid_subscription_aggregate.nbr_of_paid_subscriptions,
      CASE 
        WHEN (prep.responsible_for_group_saas_free_tier
              OR prep.individual_namespace_is_saas_free_tier
              OR prep.group_owner_of_saas_free_tier
             ) 
             AND NOT (prep.responsible_for_group_saas_ultimate_tier
                      OR prep.responsible_for_group_saas_premium_tier
                      OR prep.responsible_for_group_saas_bronze_tier
                      OR prep.individual_namespace_is_saas_bronze_tier
                      OR prep.individual_namespace_is_saas_premium_tier
                      OR prep.individual_namespace_is_saas_ultimate_tier
                      OR prep.group_owner_of_saas_bronze_tier
                      OR prep.group_owner_of_saas_premium_tier
                      OR prep.group_owner_of_saas_ultimate_tier
                     )
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_free_tier_only,
      marketing_contact.email_address,
      marketing_contact.first_name,
      IFNULL(marketing_contact.last_name, 'Unknown')                                             AS last_name,
      marketing_contact.gitlab_user_name,
      IFNULL(marketing_contact.company_name, 'Unknown')                                          AS company_name,
      marketing_contact.sfdc_record_id,
      marketing_contact.dim_crm_account_id,
      marketing_contact.job_title,
      marketing_contact.country,
      marketing_contact.sfdc_parent_sales_segment,
      marketing_contact.sfdc_parent_crm_account_tsp_region,
      marketing_contact.is_sfdc_lead_contact,
      marketing_contact.sfdc_lead_contact,
      marketing_contact.sfdc_created_date,
      marketing_contact.is_sfdc_opted_out,
      marketing_contact.is_gitlab_dotcom_user,
      marketing_contact.gitlab_dotcom_user_id,
      marketing_contact.gitlab_dotcom_created_date,
      marketing_contact.gitlab_dotcom_confirmed_date,
      marketing_contact.gitlab_dotcom_active_state,
      marketing_contact.gitlab_dotcom_last_login_date,
      marketing_contact.gitlab_dotcom_email_opted_in,
      marketing_contact.days_since_saas_signup,
      marketing_contact.is_customer_db_user,
      marketing_contact.customer_db_customer_id,
      marketing_contact.customer_db_created_date,
      marketing_contact.customer_db_confirmed_date,
      marketing_contact.days_since_self_managed_owner_signup,
      marketing_contact.zuora_contact_id,
      marketing_contact.zuora_created_date,
      marketing_contact.zuora_active_state,
      marketing_contact.wip_is_valid_email_address,
      marketing_contact.wip_invalid_email_address_reason,
      usage_metrics.smau_manage_analytics_total_unique_counts_monthly,
      usage_metrics.smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
      usage_metrics.smau_create_repo_writes,
      usage_metrics.smau_verify_ci_pipelines_users_28_days,
      usage_metrics.smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
      usage_metrics.smau_release_release_creation_users_28_days,
      usage_metrics.smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
      usage_metrics.smau_monitor_incident_management_activer_user_28_days,
      usage_metrics.smau_secure_secure_scanners_users_28_days,
      usage_metrics.smau_protect_container_scanning_jobs_users_28_days,
      usage_metrics.usage_umau_28_days_user,
      usage_metrics.usage_action_monthly_active_users_project_repo_28_days_user,
      usage_metrics.usage_merge_requests_28_days_user,
      usage_metrics.usage_commit_comment_all_time_event,
      usage_metrics.usage_source_code_pushes_all_time_event,
      usage_metrics.usage_ci_pipelines_28_days_user,
      usage_metrics.usage_ci_internal_pipelines_28_days_user,
      usage_metrics.usage_ci_builds_28_days_user,
      usage_metrics.usage_ci_builds_all_time_user,
      usage_metrics.usage_ci_builds_all_time_event,
      usage_metrics.usage_ci_runners_all_time_event,
      usage_metrics.usage_auto_devops_enabled_all_time_event,
      usage_metrics.usage_template_repositories_all_time_event,
      usage_metrics.usage_ci_pipeline_config_repository_28_days_user,
      usage_metrics.usage_user_unique_users_all_secure_scanners_28_days_user,
      usage_metrics.usage_user_container_scanning_jobs_28_days_user,
      usage_metrics.usage_user_sast_jobs_28_days_user,
      usage_metrics.usage_user_dast_jobs_28_days_user,
      usage_metrics.usage_user_dependency_scanning_jobs_28_days_user,
      usage_metrics.usage_user_license_management_jobs_28_days_user,
      usage_metrics.usage_user_secret_detection_jobs_28_days_user,
      usage_metrics.usage_projects_with_packages_all_time_event,
      usage_metrics.usage_projects_with_packages_28_days_user,
      usage_metrics.usage_deployments_28_days_user,
      usage_metrics.usage_releases_28_days_user,
      usage_metrics.usage_epics_28_days_user,
      usage_metrics.usage_issues_28_days_user,
      usage_metrics.usage_instance_user_count_not_aligned,
      usage_metrics.usage_historical_max_users_not_aligned,
      'Raw'                                                                                      AS lead_status,
      'Snowflake Email Marketing Database'                                                       AS lead_source      
    FROM prep
    LEFT JOIN marketing_contact 
      ON marketing_contact.dim_marketing_contact_id = prep.dim_marketing_contact_id
    LEFT JOIN subscription_aggregate
      ON subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN paid_subscription_aggregate
      ON paid_subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN usage_metrics
      ON usage_metrics.dim_marketing_contact_id = prep.dim_marketing_contact_id

)

{{ hash_diff(
    cte_ref="joined",
    return_cte="final",
    columns=[
      'is_group_namespace_owner',
      'is_group_namespace_member',
      'is_individual_namespace_owner',
      'is_customer_db_owner',
      'is_zuora_billing_contact',
      'days_since_saas_trial_ended',
      'days_until_saas_trial_ends',
      'individual_namespace_is_saas_trial',
      'individual_namespace_is_saas_free_tier',
      'individual_namespace_is_saas_bronze_tier',
      'individual_namespace_is_saas_premium_tier',
      'individual_namespace_is_saas_ultimate_tier',
      'group_member_of_saas_trial',
      'group_member_of_saas_free_tier',
      'group_member_of_saas_bronze_tier',
      'group_member_of_saas_premium_tier',
      'group_member_of_saas_ultimate_tier',
      'group_owner_of_saas_trial',
      'group_owner_of_saas_free_tier',
      'group_owner_of_saas_bronze_tier',
      'group_owner_of_saas_premium_tier',
      'group_owner_of_saas_ultimate_tier',
      'responsible_for_group_saas_trial',
      'responsible_for_group_saas_free_tier',
      'responsible_for_group_saas_bronze_tier',
      'responsible_for_group_saas_premium_tier',
      'responsible_for_group_saas_ultimate_tier',
      'is_self_managed_starter_tier',
      'is_self_managed_premium_tier',
      'is_self_managed_ultimate_tier',
      'min_subscription_start_date',
      'max_subscription_end_date',
      'nbr_of_paid_subscriptions',
      'email_address',
      'first_name',
      'last_name',
      'gitlab_user_name',
      'company_name',
      'job_title',
      'country',
      'sfdc_parent_sales_segment',
      'is_sfdc_lead_contact',
      'sfdc_lead_contact',
      'sfdc_created_date',
      'is_sfdc_opted_out',
      'is_gitlab_dotcom_user',
      'gitlab_dotcom_user_id',
      'gitlab_dotcom_created_date',
      'gitlab_dotcom_confirmed_date',
      'gitlab_dotcom_active_state',
      'gitlab_dotcom_last_login_date',
      'gitlab_dotcom_email_opted_in',
      'is_customer_db_user',
      'customer_db_customer_id',
      'customer_db_created_date',
      'customer_db_confirmed_date',
      'zuora_contact_id',
      'zuora_created_date',
      'zuora_active_state',
      'wip_is_valid_email_address',
      'wip_invalid_email_address_reason',
      'smau_manage_analytics_total_unique_counts_monthly',
      'smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly',
      'smau_create_repo_writes',
      'smau_verify_ci_pipelines_users_28_days',
      'smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly',
      'smau_release_release_creation_users_28_days',
      'smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly',
      'smau_monitor_incident_management_activer_user_28_days',
      'smau_secure_secure_scanners_users_28_days',
      'smau_protect_container_scanning_jobs_users_28_days',
      'usage_umau_28_days_user',
      'usage_action_monthly_active_users_project_repo_28_days_user',
      'usage_merge_requests_28_days_user',
      'usage_commit_comment_all_time_event',
      'usage_source_code_pushes_all_time_event',
      'usage_ci_pipelines_28_days_user',
      'usage_ci_internal_pipelines_28_days_user',
      'usage_ci_builds_28_days_user',
      'usage_ci_builds_all_time_user',
      'usage_ci_builds_all_time_event',
      'usage_ci_runners_all_time_event',
      'usage_auto_devops_enabled_all_time_event',
      'usage_template_repositories_all_time_event',
      'usage_ci_pipeline_config_repository_28_days_user',
      'usage_user_unique_users_all_secure_scanners_28_days_user',
      'usage_user_container_scanning_jobs_28_days_user',
      'usage_user_sast_jobs_28_days_user',
      'usage_user_dast_jobs_28_days_user',
      'usage_user_dependency_scanning_jobs_28_days_user',
      'usage_user_license_management_jobs_28_days_user',
      'usage_user_secret_detection_jobs_28_days_user',
      'usage_projects_with_packages_all_time_event',
      'usage_projects_with_packages_28_days_user',
      'usage_deployments_28_days_user',
      'usage_releases_28_days_user',
      'usage_epics_28_days_user',
      'usage_issues_28_days_user',
      'usage_instance_user_count_not_aligned',
      'usage_historical_max_users_not_aligned'
      ]
) }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@jpeguero",
    created_date="2021-02-09",
    updated_date="2021-05-07"
) }}


