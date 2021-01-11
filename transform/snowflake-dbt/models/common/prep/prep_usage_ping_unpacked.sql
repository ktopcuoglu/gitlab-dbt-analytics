{{ config({
    "materialized": "table"
    })
}}

WITH prep_usage_ping AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}

), dim_licenses AS (
  
    SELECT DISTINCT 
        license_md5, 
        subscription_id, 
        license_user_count, 
        is_trial, 
        license_start_date,
        license_expire_date 
    FROM {{ ref('dim_licenses') }}
  
), usage_pings_with_license_md5 AS (

    SELECT

      -- usage ping meta data 
      dim_usage_ping_id, 
      ping_created_at,
      ping_created_at_28_days_earlier,
      ping_created_at_year,
      ping_created_at_month,
      ping_created_at_week,
      ping_created_at_date,

      -- instance settings 
      raw_usage_data_payload:uuid, 
      ping_source, 
      raw_usage_data_payload:version                                                                            AS instance_version, 
      cleaned_version,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      edition, 
      main_edition, 
      raw_usage_data_payload:hostname                                                                           AS hostname, 
      raw_usage_data_payload:host_id                                                                            AS host_id, 
      raw_usage_data_payload:installation_type                                                                  AS installation_type, 
      is_internal, 
      is_staging,    
    
      -- instance user statistics 
      raw_usage_data_payload:instance_user_count                                                                AS instance_user_count, 
      raw_usage_data_payload:historical_max_users                                                               AS historical_max_users, 
      raw_usage_data_payload:license_md5                                                                        AS license_md5,

      -- usage ping data 
      raw_usage_data_payload:usage_activity_by_stage_monthly:manage:events                                      AS umau_28_days_user,                                            -- SELECT COUNT(DISTINCT "events"."author_id") FROM "events" WHERE "events"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:create:action_monthly_active_users_project_repo    AS action_monthly_active_users_project_repo_28_days_user,        -- REDIS - see issue for implementation 
      raw_usage_data_payload:usage_activity_by_stage_monthly:create:merge_requests                              AS merge_requests_28_days_user,                                  -- SELECT COUNT(DISTINCT "merge_requests"."author_id") FROM "merge_requests" WHERE "merge_requests"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:create:projects_with_repositories_enabled          AS projects_with_repositories_enabled_28_days_user,              -- SELECT COUNT(DISTINCT "projects"."creator_id")  FROM "projects" INNER JOIN "project_features" ON "project_features"."project_id" = "projects"."id" WHERE "project_features"."repository_access_level" = 20 AND "projects"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:counts:commit_comment                                                              AS commit_comment_all_time,                                      -- can't find in usage-metrics-2020-10-02.yaml
      raw_usage_data_payload:counts:source_code_pushes                                                          AS source_code_pushes_all_time,                                  -- can't find in usage-metrics-2020-10-02.yaml
      raw_usage_data_payload:usage_activity_by_stage_monthly:verify:ci_pipelines                                AS ci_pipelines_28_days_user,                                    -- SELECT COUNT(DISTINCT "ci_pipelines"."user_id") FROM "ci_pipelines" WHERE "ci_pipelines"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:verify:ci_internal_pipelines                       AS ci_internal_pipelines_28_days_user,                           -- SELECT COUNT(DISTINCT "ci_pipelines"."user_id") FROM "ci_pipelines" WHERE ("ci_pipelines"."source" IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13) OR "ci_pipelines"."source" IS NULL) AND "ci_pipelines"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:create:ci_builds                                   AS ci_builds_28_days_user_28_days_user,                          -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."created_at" BETWEEN '2020-09-02  14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage:create:ci_builds                                           AS ci_builds_all_time_user,                                      -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build'
      raw_usage_data_payload:counts:ci_builds                                                                   AS ci_builds_all_time_event,                                     -- SELECT COUNT("ci_builds"."id") FROM "ci_builds" WHERE "ci_builds"."type"= 'Ci::Build'
      raw_usage_data_payload:counts:ci_runners                                                                  AS ci_runners_all_time_event,                                    -- SELECT COUNT("ci_runners"."id") FROM "ci_runners"
      raw_usage_data_payload:counts:auto_devops_enabled                                                         AS auto_devops_enable_all_time_event,                            -- SELECT COUNT("project_auto_devops"."id") FROM "project_auto_devops"  WHERE "project_auto_devops"."enabled" = TRUE
      raw_usage_data_payload:gitlab_shared_runners_enabled                                                      AS gitlab_shared_runners_enabled_instance_setting,               -- instance setting - true or false value 
      raw_usage_data_payload:container_registry_enabled                                                         AS container_registry_enabled_instance_setting,                  -- instance setting - true or false value 
      raw_usage_data_payload:counts:template_repositories                                                       AS template_repositories_all_time_event,                         -- SELECT COUNT("projects"."id") FROM "projects" WHERE "projects"."namespace_id" IS NULL SELECT COUNT("projects"."id") FROM "projects" INNER JOIN namespaces ON projects.namespace_id = namespaces.custom_project_templates_group_id
      raw_usage_data_payload:usage_activity_by_stage_monthly:create:ci_pipeline_config_repository               AS ci_pipeline_config_repository_28_days_user,                   -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."created_at" BETWEEN '2020-09-02  14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_unique_users_all_secure_scanners       AS user_unique_users_all_secure_scanners_28_days_user,           -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" IN ('container_scanning', 'dast', 'dependency_scanning', 'license_management', 'license_scanning', 'sast', 'secret_detection', 'coverage_fuzzing') AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_sast_jobs                              AS user_sast_jobs_28_days_user,                                  -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" = 'sast' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_dast_jobs                              AS user_dast_jobs_28_days_user,                                  -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" = 'dast' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_dependency_scanning_jobs               AS user_dependency_scanning_jobs_28_days_user,                   -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" =   'dependency_scanning' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773'   AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_license_management_jobs                AS user_license_management_jobs_28_days_user,                    -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" =    'license_management' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_secret_detection_jobs                  AS user_secret_detection_jobs_28_days_user,                      -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" = 'secret_detection' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:secure:user_container_scanning_jobs                AS user_container_scanning_job_28_days_users,                    -- SELECT COUNT(DISTINCT "ci_builds"."user_id") FROM  "ci_builds" WHERE "ci_builds"."type" = 'Ci::Build' AND "ci_builds"."name" =    'container_scanning' AND "ci_builds"."created_at" BETWEEN '2020-09-02 14:27:44.648773'  AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:object_store:packages:enabled                                                      AS object_store_packages_enabled_instance_setting,               -- instance setting - true or false value 
      raw_usage_data_payload:counts:projects_with_packages                                                      AS projects_with_packages_all_time_event,                        -- SELECT COUNT(DISTINCT "packages_packages"."project_id")  FROM "packages_packages"
      raw_usage_data_payload:usage_activity_by_stage_monthly:package:projects_with_packages                     AS projects_with_packages_28_days_users,                         -- SELECT COUNT(DISTINCT "projects"."creator_id") FROM "projects" INNER JOIN "packages_packages" ON "packages_packages"."project_id" = "projects"."id" WHERE "projects"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30  14:27:44.648831'  
      raw_usage_data_payload:usage_activity_by_stage_monthly:release:deployments                                AS deployments_28_days_users,                                    -- SELECT COUNT(DISTINCT "deployments"."user_id") FROM "deployments" WHERE "deployments"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:release:releases                                   AS releases_28_days_users,                                       -- SELECT COUNT(DISTINCT "releases"."author_id") FROM "releases" WHERE "releases"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:plan:epics                                         AS epics_28_days_users,                                          -- SELECT COUNT(DISTINCT "epics"."author_id") FROM "epics" WHERE "epics"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      raw_usage_data_payload:usage_activity_by_stage_monthly:plan:issues                                        AS issues_28_days_users                                         -- SELECT COUNT(DISTINCT "issues"."author_id") FROM "issues" WHERE "issues"."created_at" BETWEEN '2020-09-02 14:27:44.648773' AND '2020-09-30 14:27:44.648831'
      --raw_usage_data_payload
    FROM prep_usage_ping
    WHERE license_md5 IS NOT NULL 
  
), 

SELECT 
    
FROM prep_usage_ping
