WITH gitlab_dotcom_date AS (

    {% set tables = ['alert_management_alerts' , 'application_settings' , 'approval_merge_request_rules' ,'approvals' , 'boards' , 'ci_builds' , 'ci_group_variables' , 'ci_job_artifacts' , 'ci_pipeline_schedule_variables' , 'ci_pipeline_schedules' , 'ci_pipelines' , 'ci_runner_projects' , 'ci_runners' , 'ci_stages' , 'ci_trigger_requests' , 'ci_triggers' , 'cluster_projects' , 'clusters'  , 'clusters_applications_crossplane' ,  'clusters_applications_helm' , 'clusters_applications_jupyter'  , 'deployments' , 'elasticsearch_indexed_namespaces' , 'emails' , 'environments'  , 'epics' , 'events' , 'experiment_subjects' , 'gitlab_subscriptions' , 'group_group_links' , 'identities' , 'in_product_marketing_emails' , 'issue_links' , 'issue_metrics' , 'issues' , 'keys' , 'label_links' , 'label_priorities' , 'labels' , 'lfs_objects_projects' ,  'lists' , 'merge_request_diffs' , 'merge_request_metrics' , 'merge_requests' , 'merge_requests_closing_issues' , 'milestones' , 'namespace_root_storage_statistics' , 'namespace_settings' , 'namespaces' , 'notes' , 'notification_settings' ,  'onboarding_progresses' , 'packages_packages' , 'plans' , 'project_auto_devops' , 'project_features' , 'project_group_links' , 'project_repository_storage_moves' , 'projects' , 'prometheus_alerts' , 'protected_branches' , 'push_rules' , 'releases' , 'requirements'  , 'services'  , 'sprints' , 'subscriptions' , 'system_note_metadata' , 'snippets' , 'terraform_states' , 'todos' , 'user_preferences' , 'users' , 'user_custom_attributes' , 'vulnerabilities' , 'vulnerability_occurrences' , 'alert_management_http_integrations' , 'analytics_cycle_analytics_group_stages' , 'approval_project_rules' , 'bulk_imports' , 'cluster_agent_tokens' , 'cluster_agents' , 'container_expiration_policies' , 'csv_issue_imports' , 'grafana_integrations' , 'group_import_states' , 'jira_imports' ,  'jira_tracker_data' , 'ldap_group_links' , 'lfs_objects' , 'operations_feature_flags' , 'path_locks' , 'remote_mirrors' , 'security_scans' , 'status_page_published_incidents' , 'users_ops_dashboard_projects' , 'boards_epic_boards' ] %}					

    {% for table in tables %} 
    SELECT '{{table}}'                                                   AS table_name,
        MAX(date(updated_at))                                            AS max_date 
    FROM {{source('gitlab_dotcom', table)}}  
  
  
    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %} 
  
)


  SELECT *
  FROM gitlab_dotcom_date