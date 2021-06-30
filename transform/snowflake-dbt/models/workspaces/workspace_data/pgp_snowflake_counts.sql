{{ config({
        "materialized": "table"
    })
}}


WITH postgres_counts AS (

    SELECT
        table_name,
        created_date,
        updated_date,
        number_of_records
    FROM {{source('gitlab_dotcom','gitlab_pgp_export')}} 
    WHERE table_name NOT IN (
        'gitlab_db_operations_feature_flags',
        'gitlab_db_requirements_management_test_reports',
        'gitlab_db_resource_milestone_events',
        'gitlab_db_resource_weight_events',
        'gitlab_db_authentication_events',
        'gitlab_db_uploads')
    GROUP BY 1,2,3,4
    QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name,created_date,updated_date ORDER BY updated_date DESC) = 1
                  ORDER BY table_name, updated_date DESC

), snowflake_sub AS (

    SELECT 
        id,
        'gitlab_db_in_product_marketing_emails'                           AS table_name,  
        DATE(created_at)                                                  AS created_date,
        DATE(updated_at)                                                  AS updated_date
    FROM {{source('gitlab_dotcom','in_product_marketing_emails')}} 
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_in_product_marketing_emails')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT 
        id,
        'gitlab_db_clusters'                                             AS table_name, 
        DATE(created_at)                                                 AS created_date,
        DATE(updated_at)                                                 AS updated_date
    FROM  {{source('gitlab_dotcom','clusters')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT 
        id,
        'gitlab_db_cluster_projects'                                    AS table_name, 
        DATE(created_at)                                                AS created_date,
        DATE(updated_at)                                                AS updated_date
    FROM  {{source('gitlab_dotcom','cluster_projects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_cluster_projects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_sprints'                                          AS table_name, 
        DATE(created_at)                                               AS created_date,
        DATE(updated_at)                                               AS updated_date
    FROM  {{source('gitlab_dotcom','sprints')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_sprints')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_runners'                      AS table_name, 
        DATE(created_at)                                               AS created_date,
        DATE(updated_at)                                               AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_runners')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_runners')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_users_ops_dashboard_projects'                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','users_ops_dashboard_projects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_users_ops_dashboard_projects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_prometheus'                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_prometheus')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_prometheus')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_prometheus_alerts'                                AS table_name, 
        DATE(created_at)                                             AS created_date,
        DATE(updated_at)                                             AS updated_date
    FROM  {{source('gitlab_dotcom','prometheus_alerts')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_prometheus_alerts')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_ingress'                   AS table_name, 
        DATE(created_at)                                            AS created_date,
        DATE(updated_at)                                            AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_ingress')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_ingress')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_helm'                      AS table_name, 
        DATE(created_at)                                            AS created_date,
        DATE(updated_at)                                            AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_helm')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_helm')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_path_locks'                                       AS table_name, 
        DATE(created_at)                                             AS created_date,
        DATE(updated_at)                                             AS updated_date
    FROM  {{source('gitlab_dotcom','path_locks')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_path_locks')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_csv_issue_imports'                                 AS table_name,                                   
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','csv_issue_imports')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_csv_issue_imports')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_cluster_providers_gcp'                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','cluster_providers_gcp')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_cluster_provid,ers_gcp')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_requirements'                                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','requirements')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_requirements')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_cert_managers'               AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_cert_managers')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_cert_managers')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT issue_id,
        'gitlab_db_issues_prometheus_alert_events'                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','issues_prometheus_alert_events')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_issues_prometheus_alert_events')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_elastic_stacks'            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_elastic_stacks')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_elastic_stacks')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT group_id,
        'gitlab_db_group_import_states'                               AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','group_import_states')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_group_import_states')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_knative'                     AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_knative')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_knative')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_analytics_cycle_analytics_group_stages'            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','analytics_cycle_analytics_group_stages')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_analytics_cycle_analytics_group_stages')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_cluster_providers_aws'                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','cluster_providers_aws')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_cluster_providers_gcp')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_jira_imports'                                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','jira_imports')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_jira_imports')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_bulk_imports'                                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','bulk_imports')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_bulk_imports')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_crossplane'                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_crossplane')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_crossplane')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_alert_management_http_integrations'                AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','alert_management_http_integrations')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_alert_management_http_integrations')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_alert_management_http_integrations'                AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','alert_management_http_integrations')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_alert_management_http_integrations')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_jupyter'                     AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_jupyter')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_jupyter')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_zoom_meetings'                                     AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','zoom_meetings')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_zoom_meetings')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_grafana_integrations'                              AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','grafana_integrations')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_grafana_integrations')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_epic_metrics'                                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','epic_metrics')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_epic_metrics')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_project_tracing_settings'                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','project_tracing_settings')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_project_tracing_settings')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_application_settings'                              AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','application_settings')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_application_settings')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_cluster_agents'                                    AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','cluster_agents')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_cluster_agents')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_cluster_agent_tokens'                              AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','cluster_agent_tokens')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_cluster_agent_tokens')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT issue_id,
        'gitlab_db_issues_self_managed_prometheus_alert_events'       AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','issues_self_managed_prometheus_alert_events')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_issues_self_managed_prometheus_alert_events')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ldap_group_links'                                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ldap_group_links')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ldap_group_links')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_clusters_applications_cilium'                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','clusters_applications_cilium')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_clusters_applications_cilium')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_status_page_published_incidents'                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','status_page_published_incidents')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_status_page_published_incidents')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT project_id,
        'gitlab_db_status_page_settings'                              AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','status_page_settings')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_status_page_settings')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_licenses'                                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','licenses')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_licenses')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_alert_management_alerts'                           AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','alert_management_alerts')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_alert_management_alerts')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_epics'                                             AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','epics')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_epics')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_snippets'                                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','snippets')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_snippets')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_emails'                                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','emails')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_emails')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_jira_tracker_data'                                 AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','jira_tracker_data')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_jira_tracker_data')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_onboarding_progresses'                             AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','onboarding_progresses')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_onboarding_progresses')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_approval_project_rules'                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','approval_project_rules')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_approval_project_rules')
    GROUP BY 1,2,3,4
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_label_priorities'                                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','label_priorities')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_label_priorities')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_approver_groups'                                   AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','approver_groups')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_approver_groups')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_triggers'                                       AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_triggers')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_triggers')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_terraform_states'                                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','terraform_states')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_terraform_states')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_releases'                                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','releases')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_releases')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_boards'                                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','boards')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_boards')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_runner_projects'                                AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_runner_projects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_runner_projects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_project_auto_devops'                               AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','project_auto_devops')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_project_auto_devops')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_packages_packages'                                 AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','packages_packages')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_packages_packages')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_runners'                                        AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_runners')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_runners')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_subscriptions'                                     AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','subscriptions')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_subscriptions')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_user_custom_attributes'                            AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','user_custom_attributes')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_user_custom_attributes')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_services'                                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','services')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_services')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_group_variables'                                AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_group_variables')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_group_variables')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_security_scans'                                    AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','security_scans')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_security_scans')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
        SELECT id,
        'gitlab_db_project_custom_attributes'                         AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','project_custom_attributes')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_project_custom_attributes')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_environments'                                      AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','environments')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_environments')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_push_rules'                                        AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','push_rules')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_push_rules')
    GROUP BY 1,2,3,4
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_identities'                                       AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','identities')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_identities')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_experiment_users'                                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','experiment_users')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_experiment_users')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_experiment_users'                                  AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','experiment_users')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_experiment_users')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_timelogs'                                          AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','timelogs')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_timelogs')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_milestones'                                        AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','milestones')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_milestones')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_pipeline_schedule_variables'                    AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_pipeline_schedule_variables')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_pipeline_schedule_variables')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
        'gitlab_db_ci_pipeline_schedule_variables'                    AS table_name, 
        DATE(created_at)                                              AS created_date,
        DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_pipeline_schedule_variables')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_pipeline_schedule_variables')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_vulnerabilities'                                   AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','vulnerabilities')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_vulnerabilities')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_namespaces'                                        AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','namespaces')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_namespaces')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_merge_requests_closing_issues'                     AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','merge_requests_closing_issues')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_merge_requests_closing_issues')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT user_id,
      'gitlab_db_user_preferences'                                  AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','user_preferences')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_user_preferences')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_lists'                                             AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','lists')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_lists')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT project_id,
      'gitlab_db_container_expiration_policies'                     AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','container_expiration_policies')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_container_expiration_policies')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_approval_merge_request_rules'                      AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','approval_merge_request_rules')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_approval_merge_request_rules')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_protected_branches'                                AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','protected_branches')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_protected_branches')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_projects'                                          AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','projects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_projects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_labels'                                            AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','labels')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_labels')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_ci_trigger_requests'                               AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','ci_trigger_requests')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_ci_trigger_requests')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_lfs_objects'                                       AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','lfs_objects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_lfs_objects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_project_features'                                  AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','project_features')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_project_features')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
      'gitlab_db_approvals'                                         AS table_name, 
      DATE(created_at)                                              AS created_date,
      DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','approvals')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_approvals')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_lfs_objects_projects'                              AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','lfs_objects_projects')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_lfs_objects_projects')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_merge_request_metrics'                             AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','merge_request_metrics')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_merge_request_metrics')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_users'                                             AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','users')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_users')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_notification_settings'                             AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','notification_settings')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_notification_settings')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_todos'                                             AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','todos')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_todos')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1
    UNION ALL
    SELECT id,
       'gitlab_db_issue_metrics'                                     AS table_name, 
       DATE(created_at)                                              AS created_date,
       DATE(updated_at)                                              AS updated_date
    FROM  {{source('gitlab_dotcom','issue_metrics')}}
    WHERE DATE(updated_at)  >= (SELECT DATEADD(day, -8, max(updated_date)) FROM {{source('gitlab_dotcom','gitlab_pgp_export')}}  WHERE table_name = 'gitlab_db_issue_metrics')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_date DESC) = 1


), snowflake_counts AS (

    SELECT
        table_name,
        created_date,
        updated_date,
        COUNT(*) AS number_of_records
    FROM snowflake_sub
    GROUP BY 1,2,3

), comparision AS (

    SELECT
       snowflake_counts.table_name,
       snowflake_counts.created_date AS created_date,
       snowflake_counts.updated_date AS updated_date,
       postgres_counts.number_of_records AS postgres_counts,
       snowflake_counts.number_of_records AS snowflake_counts
    FROM snowflake_counts 
    INNER JOIN POSTGRES_COUNTS
    ON snowflake_counts.table_name = postgres_counts.table_name
    AND snowflake_counts.created_date = postgres_counts.created_date
    AND snowflake_counts.updated_date = SUBSTRING(postgres_counts.updated_date,1,10)
)

    SELECT *,
        postgres_counts-snowflake_counts AS DEVIATION
    FROM comparision
    ORDER BY table_name, updated_date DESC

  
