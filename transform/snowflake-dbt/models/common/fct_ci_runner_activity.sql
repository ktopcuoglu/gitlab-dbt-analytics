{{ config(
    tags=["product"]
) }}


{{ simple_cte([
    ('dim_ci_build', 'dim_ci_build'),
    ('dim_ci_runner', 'dim_ci_runner'),
    ('dim_ci_pipeline', 'dim_ci_pipeline'),
    ('dim_ci_stage', 'dim_ci_stage'),
    ('dim_project', 'dim_project'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_user', 'dim_user'),
    ('dim_date', 'dim_date')
]) }}

, joined AS (

    SELECT
      -- PRIMARY KEY
      dim_ci_build.dim_ci_build_id,

      -- FOREIGN KEYS
      {{ get_keyed_nulls('dim_ci_runner.dim_ci_runner_id') }}       AS dim_ci_runner_id,
      {{ get_keyed_nulls('dim_ci_pipeline.dim_ci_pipeline_id') }}   AS dim_ci_pipeline_id,
      {{ get_keyed_nulls('dim_ci_stage.dim_ci_stage_id') }}         AS dim_ci_stage_id,
      {{ get_keyed_nulls('dim_project.dim_project_id') }}           AS dim_project_id,
      {{ get_keyed_nulls('dim_user.dim_user_id') }}                 AS dim_user_id,
      {{ get_keyed_nulls('dim_date.date_id') }}                     AS created_date_id,
      {{ get_keyed_nulls('dim_namespace.dim_namespace_id') }}       AS dim_namespace_id,
      dim_namespace.ultimate_parent_namespace_id,
      dim_ci_build.dim_plan_id,

      -- ci_build metadata
      DATEDIFF('seconds',
               dim_ci_build.started_at,
               dim_ci_build.finished_at)                            AS ci_build_duration_in_s,
      dim_ci_build.ci_build_status,

      -- ci_runner metadata
      -- MACRO for v1
      CASE dim_ci_runner.runner_type
        WHEN 1 THEN 'shared'
        WHEN 2 THEN 'group-runner-hosted runners'
        WHEN 3 THEN 'project-runner-hosted runners' 
      END                                                           AS runner_type_summary,
      CASE 
        WHEN dim_ci_runner.ci_runner_description LIKE 'private-runners-manager%'
          THEN 'private-runner-mgr'
        WHEN dim_ci_runner.ci_runner_description LIKE 'shared-runners-manager%'
          THEN 'linux-runner-mgr'
        WHEN dim_ci_runner.ci_runner_description LIKE 'gitlab-shared-runners-manager%'
          THEN 'gitlab-internal-runner-mgr'
        WHEN dim_ci_runner.ci_runner_description LIKE 'windows-shared-runners-manager%'
          THEN 'windows-runner-mgr'
          ELSE 'Other'
      END                                                           AS runner_manager,
      CASE
        WHEN dim_namespace.namespace_is_internal = TRUE
          THEN TRUE
        WHEN dim_ci_runner.runner_type = 1
          THEN TRUE
          ELSE FALSE
      END                                                           AS is_paid_by_gitlab,
      dim_ci_runner.public_projects_minutes_cost_factor,
      dim_ci_runner.private_projects_minutes_cost_factor,

      -- project metadata
      dim_project.visibility_level                                  AS project_visibility_level,
      dim_project.project_path

    FROM dim_ci_build
    LEFT JOIN dim_ci_runner 
      ON dim_ci_build.ci_runner_id = dim_ci_runner.dim_ci_runner_id
    LEFT JOIN dim_ci_pipeline
      ON dim_ci_build.dim_ci_pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
    LEFT JOIN dim_ci_stage 
      ON dim_ci_pipeline.dim_ci_pipeline_id = dim_ci_stage.dim_ci_pipeline_id
    LEFT JOIN dim_project 
      ON dim_ci_build.dim_project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace
      ON dim_ci_build.dim_namespace_id = dim_namespace.dim_namespace_id
    LEFT JOIN dim_user
      ON dim_ci_build.dim_user_id = dim_user.dim_user_id
    LEFT JOIN dim_date
      ON dim_ci_build.created_date_id = dim_date.date_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-06-30",
    updated_date="2021-07-09"
) }}
