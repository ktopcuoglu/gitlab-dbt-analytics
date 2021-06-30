{{ config(
    tags=["product"]
) }}


{{ simple_cte([
    ('dim_ci_build', 'dim_ci_build'),
    ('dim_project', 'dim_project'),
    ('gitlab_dotcom_ci_runners', 'gitlab_dotcom_ci_runners')
]) }}

, joined AS (

    SELECT
      -- PRIMAry KEY
      dim_ci_build.dim_ci_build_id,

      -- FOREIGN KEYS
      dim_ci_build.created_date_id,
      dim_ci_build.ci_runner_id,
      dim_ci_build.dim_user_id,
      dim_ci_build.ultimate_parent_namespace_id,
      dim_ci_build.dim_project_id,
      dim_plan_id,

      -- ci_build metadata
      DATEDIFF('seconds', started_at, finished_at) AS ci_build_duration_in_s,
      dim_ci_build.status                          AS ci_build_status,

      -- ci_runner metadata
      -- MACRO for v1 CASE
        WHEN gitlab_dotcom_ci_runners.runner_type = 1 THEN 'shared'
        WHEN gitlab_dotcom_ci_runners.runner_type = 2 THEN  'group-runner-hosted runners'
        WHEN gitlab_dotcom_ci_runners.runner_type = 3 THEN  'project-runner-hosted runners' 
      END                                          AS runner_type,
      CASE 
          WHEN gitlab_dotcom_ci_runners.description LIKE 'private-runners-manager%' THEN 'private-runner-mgr'
          WHEN gitlab_dotcom_ci_runners.description LIKE 'shared-runners-manager%' THEN 'linux-runner-mgr'
          WHEN gitlab_dotcom_ci_runners.description LIKE 'gitlab-shared-runners-manager%' THEN 'gitlab-internal-runner-mgr'
          WHEN gitlab_dotcom_ci_runners.description LIKE 'windows-shared-runners-manager%' THEN 'windows-runner-mgr'
          ELSE 'Other'
        END                                        AS runner_manager,
      CASE 
        WHEN namespace_is_internal = TRUE THEN TRUE
        WHEN gitlab_dotcom_ci_runners.runner_type = 1 THEN TRUE
        ELSE False
      END                                          AS is_paid_by_gitlab,
      public_projects_minutes_cost_factor,
      private_projects_minutes_cost_factor,

      namespace_is_internal,
      dim_project.visibility_level                 AS project_visibility_level,
      dim_project.project_path
    FROM dim_ci_build
    LEFT JOIN gitlab_dotcom_ci_runners 
      ON dim_ci_build.ci_runner_id = gitlab_dotcom_ci_runners.runner_id
    LEFT JOIN dim_project 
      ON dim_ci_build.dim_project_id = dim_project.dim_project_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-30",
    updated_date="2021-06-30"
) }}

