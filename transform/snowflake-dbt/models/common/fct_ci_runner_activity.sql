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
      IFNULL(dim_ci_runner.dim_ci_runner_id, -1)              AS dim_ci_runner_id,
      IFNULL(dim_ci_pipeline.dim_ci_pipeline_id, -1)          AS dim_ci_pipeline_id,
      IFNULL(dim_ci_stage.dim_ci_stage_id, -1)                AS dim_ci_stage_id,
      IFNULL(dim_project.dim_project_id, -1)                  AS dim_project_id,
      IFNULL(dim_user.dim_user_id, -1)                        AS dim_user_id,
      IFNULL(dim_date.date_id, -1)                            AS created_date_id,
      IFNULL(dim_namespace.dim_namespace_id, -1)              AS dim_namespace_id,
      IFNULL(dim_namespace.ultimate_parent_namespace_id, -1)  AS ultimate_parent_namespace_id,
      dim_ci_build.dim_plan_id,

      -- ci_build metrics
      DATEDIFF('seconds',
               dim_ci_build.started_at,
               dim_ci_build.finished_at)                      AS ci_build_duration_in_s,

      -- ci_runner metrics
      CASE
        WHEN dim_namespace.namespace_is_internal = TRUE
          THEN TRUE
        WHEN dim_ci_runner.runner_type = 1
          THEN TRUE
          ELSE FALSE
      END                                                     AS is_paid_by_gitlab,
      dim_ci_runner.public_projects_minutes_cost_factor,
      dim_ci_runner.private_projects_minutes_cost_factor

    FROM dim_ci_build
    LEFT JOIN dim_ci_runner 
      ON dim_ci_build.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
    LEFT JOIN dim_ci_stage
      ON dim_ci_build.dim_ci_stage_id = dim_ci_stage.dim_ci_stage_id
    LEFT JOIN dim_ci_pipeline
      ON dim_ci_stage.dim_ci_pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
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
    updated_date="2021-07-14"
) }}
