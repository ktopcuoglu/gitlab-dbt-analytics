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
      dim_ci_build.dim_ci_build_id,
      dim_ci_build.created_date_id,
      dim_ci_build.ci_runner_id,
      dim_ci_build.dim_user_id,
      DATEDIFF('seconds', started_at, finished_at) AS ci_build_duration_in_s,
      dim_ci_build.ultimate_parent_namespace_id,
      dim_ci_build.dim_project_id,
      dim_plan_id,

      namespace_is_internal,
      public_projects_minutes_cost_factor,
      private_projects_minutes_cost_factor,
      IFF(gitlab_dotcom_ci_runners.runner_type = 1, 'Shared Runners', 'Self-hosted runners') AS runner_typ,
      CASE 
        WHEN gitlab_dotcom_ci_runners.description LIKE 'windows-shared-runners-manager%' THEN 'windows-runner-mgr'
        WHEN gitlab_dotcom_ci_runners.description LIKE 'shared-runners-manager%' THEN 'linux-runner-mgr'
        ELSE 'Other'
      END AS runner_manager,
      dim_ci_build.status
    FROM dim_ci_build
    LEFT JOIN gitlab_dotcom_ci_runners 
      ON dim_ci_build.ci_runner_id = gitlab_dotcom_ci_runners.runner_id


)

SELECT *
FROM joined
