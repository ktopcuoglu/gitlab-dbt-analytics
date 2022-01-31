{{ config({
        "materialized": "table"
    })
}}


WITH postgres_counts AS (

    SELECT table_name,
        created_date,
        updated_date,
        number_of_records,
        parse_json(_updated_at)::int::timestamp_ntz           AS updated_at
    FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}
    WHERE DATE(created_date) = DATE(updated_date)
    AND DATE(updated_at) =  current_date
    AND DATE(updated_at) = DATE(created_date) + 1
    AND  table_name NOT IN (
        'gitlab_db_operations_feature_flags',
        'gitlab_db_requirements_management_test_reports',
        'gitlab_db_resource_milestone_events',
        'gitlab_db_resource_weight_events',
        'gitlab_db_authentication_events',
        'gitlab_db_uploads',
        'gitlab_db_resource_label_events',
        'gitlab_db_lfs_file_locks',
        'gitlab_db_project_daily_statistics',
        'gitlab_db_audit_events',
        'gitlab_db_ci_platform_metrics',
        'gitlab_db_namespace_root_storage_statistics',
        'gitlab_ops_db_ci_stages')
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name,created_date,updated_date ORDER BY updated_at) = 1
                  ORDER BY table_name, updated_date DESC
),  sub_group AS (

    {% set tables = ['label_priorities', 'labels', 'ldap_group_links', 'namespaces','cluster_providers_gcp', 'packages_packages', 'ci_runner_projects', 'push_rules', 'requirements', 'todos', 'project_auto_devops', 'application_settings', 'ci_triggers', 'clusters_applications_cilium', 'clusters_applications_elastic_stacks', 'users', 'zoom_meetings', 'alert_management_http_integrations', 'approval_project_rules', 'clusters', 'issue_metrics', 'jira_tracker_data', 'lists', 'sprints', 'users_ops_dashboard_projects', 'bulk_imports', 'cluster_agent_tokens',  'experiment_users', 'protected_branches', 'timelogs', 'project_features', 'milestones', 'alert_management_alerts', 'ci_group_variables', 'cluster_agents', 'emails', 'user_custom_attributes', 'grafana_integrations', 'security_scans', 'lfs_objects_projects' , 'merge_request_metrics', 'merge_requests_closing_issues', 'path_locks', 'approval_merge_request_rules' , 'csv_issue_imports', 'cluster_projects', 'vulnerabilities', 'releases', 'subscriptions', 'terraform_states', 'project_tracing_settings', 'notification_settings', 'environments', 'epics', 'in_product_marketing_emails', 'jira_imports', 'services', 'onboarding_progresses', 'project_custom_attributes', 'analytics_cycle_analytics_group_stages', 'approvals', 'ci_pipeline_schedule_variables', 'ci_runners', 'ci_trigger_requests', 'cluster_providers_aws', 'boards', 'projects', 'identities', 'lfs_objects', 'prometheus_alerts', 'snippets', 'system_note_metadata', 'merge_request_blocks', 'merge_request_diffs', 'experiment_subjects', 'deployments', 'merge_requests', 'remote_mirrors', 'integrations', 'events', 'ci_stages', 'ci_pipelines', 'ci_job_artifacts', 'ci_pipeline_schedules','approver_groups' , 'boards_epic_boards' ,'web_hooks', 'routes' ,'geo_nodes'] %}

    {% for table in tables %}
    SELECT id,
        'gitlab_db_{{table}}'                                  AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom', table)}}                    
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1


    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}

UNION ALL

    {% set tables = ['labels', 'merge_request_metrics', 'projects', 'merge_requests', 'users','ci_pipelines'] %}

    {% for table in tables %}
    SELECT id,
        'gitlab_ops_db_{{table}}'                                 AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_ops', table)}}                        
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1


    {% if not loop.last %}
    UNION ALL
    {% endif %}

{% endfor %}

),  final_group AS (  --union all of tables with different column name for id

    SELECT *
    FROM sub_group
    UNION ALL
    SELECT issue_id,
        'gitlab_db_issues_prometheus_alert_events'                                          AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','issues_prometheus_alert_events')}}                      
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT group_id,
        'gitlab_db_group_import_states'                                                       AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','group_import_states')}}                                   
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT issue_id,
        'gitlab_db_issues_self_managed_prometheus_alert_events'                                AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','issues_self_managed_prometheus_alert_events')}}            
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT project_id,
        'gitlab_db_status_page_settings'                                                       AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','status_page_settings')}}                                   
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT user_id,
        'gitlab_db_user_preferences'                                                           AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','user_preferences')}}                                       
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT project_id,
        'gitlab_db_container_expiration_policies'                                              AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','container_expiration_policies')}}                          
     WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY uploaded_at DESC) = 1
    UNION ALL
    SELECT namespace_id,
        'gitlab_db_namespace_settings'                                                         AS table_name,
        DATE(created_at)                                       AS created_date,
        DATE(updated_at)                                       AS updated_date,
        DATE(parse_json(_uploaded_at)::int::timestamp_ntz)     AS uploaded_at
    FROM {{source('gitlab_dotcom','namespace_settings')}}                                     
    WHERE DATE(created_at) = DATE(updated_at)
    AND DATE(uploaded_at) =  current_date
    AND DATE(uploaded_at) = DATE(created_date) + 1
    GROUP BY 1,2,3,4,5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY uploaded_at DESC) = 1

), snowflake_counts AS (

    SELECT table_name,
        created_date,
        updated_date,
        COUNT(*) AS number_of_records
    FROM final_group
    GROUP BY 1,2,3

), comparision AS (

    SELECT
       snowflake_counts.table_name                         AS table_name,
       snowflake_counts.created_date                       AS created_date,
       snowflake_counts.updated_date                       AS updated_date,
       postgres_counts.number_of_records                   AS postgres_counts,
       snowflake_counts.number_of_records                  AS snowflake_counts
    FROM snowflake_counts
    INNER JOIN postgres_counts
    ON snowflake_counts.table_name = postgres_counts.table_name
    AND snowflake_counts.created_date = postgres_counts.created_date
    AND snowflake_counts.updated_date = SUBSTRING(postgres_counts.updated_date,1,10)
)

    SELECT *,
        postgres_counts-snowflake_counts AS DEVIATION
    FROM comparision
    ORDER BY table_name, updated_date DESC
