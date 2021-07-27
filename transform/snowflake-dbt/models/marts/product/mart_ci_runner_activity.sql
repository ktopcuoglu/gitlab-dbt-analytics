{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('ci_runner_activity', 'fct_ci_runner_activity'),
    ('dim_ci_build', 'dim_ci_build'),
    ('dim_ci_runner', 'dim_ci_runner'),
    ('dim_ci_pipeline', 'dim_ci_pipeline'),
    ('dim_ci_stage', 'dim_ci_stage'),
    ('dim_project', 'dim_project'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_user', 'dim_user'),
    ('dim_date', 'dim_date')
]) }}

, ci_runner_activity_monthly AS (

    SELECT 
      ci_runner_activity.dim_ci_build_id,
      ci_runner_activity.dim_ci_runner_id,
      ci_runner_activity.dim_ci_pipeline_id,
      ci_runner_activity.dim_ci_stage_id,
      ci_runner_activity.dim_project_id,
      ci_runner_activity.dim_user_id,
      dim_date.first_day_of_month,
      ci_runner_activity.dim_namespace_id,
      ci_runner_activity.ultimate_parent_namespace_id,
      ci_runner_activity.dim_plan_id,
      ci_runner_activity.public_projects_minutes_cost_factor,
      ci_runner_activity.private_projects_minutes_cost_factor,
      SUM(ci_runner_activity.ci_build_duration_in_s)            AS ci_build_duration_in_s
    FROM ci_runner_activity
    INNER JOIN dim_date
      ON ci_runner_activity.created_date_id = dim_date.date_id
    {{ dbt_utils.group_by(n=12) }}

), joined AS (

    SELECT
      -- PRIMARY KEY
      dim_ci_build.dim_ci_build_id,

      -- FOREIGN KEYS
      dim_ci_runner.dim_ci_runner_id,
      dim_ci_pipeline.dim_ci_pipeline_id,
      dim_ci_stage.dim_ci_stage_id,
      dim_project.dim_project_id,
      dim_user.dim_user_id,
      dim_date.date_id                                          AS ci_build_created_date_id,
      dim_namespace.dim_namespace_id,
      dim_namespace.ultimate_parent_namespace_id,
      dim_ci_build.dim_plan_id,

      -- CI RUNNER METRICS
      ci_runner_activity_monthly.ci_build_duration_in_s,
      ci_runner_activity_monthly.public_projects_minutes_cost_factor,
      ci_runner_activity_monthly.private_projects_minutes_cost_factor,

      -- CI RUNNER METADATA
      dim_ci_runner.ci_runner_description,
      dim_ci_runner.ci_runner_manager,
      dim_ci_runner.ci_runner_type,
      dim_ci_runner.ci_runner_type_summary,
      dim_ci_stage.ci_stage_name,
      dim_ci_build.ci_build_name,
      dim_ci_build.ci_build_status,
      dim_ci_build.created_at                                   AS ci_build_created_at,
      dim_ci_build.updated_at                                   AS ci_build_updated_at,
      dim_ci_build.scheduled_at                                 AS ci_build_scheduled_at,
      dim_ci_build.queued_at                                    AS ci_build_queued_at,
      dim_ci_build.started_at                                   AS ci_build_started_at,
      dim_ci_build.finished_at                                  AS ci_build_finished_at,
      dim_ci_build.tag                                          AS ci_build_tag,
      dim_ci_build.ref                                          AS ci_build_ref,
      dim_ci_build.ci_build_type,
      dim_ci_build.target_url                                   AS ci_build_target_url,
      dim_ci_build.ci_build_description,
      dim_ci_build.failure_reason                               AS ci_build_failure_reason,
      ci_runner_activity_monthly.is_paid_by_gitlab,
      dim_namespace.namespace_is_internal,
      dim_namespace.gitlab_plan_title                           AS ultimate_parent_plan_title

    FROM ci_runner_activity_monthly
    INNER JOIN dim_ci_build
      ON ci_runner_activity_monthly.dim_ci_build_id = dim_ci_build.dim_ci_build_id
    INNER JOIN dim_ci_runner
      ON ci_runner_activity_monthly.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
    INNER JOIN dim_ci_stage
      ON ci_runner_activity_monthly.dim_ci_stage_id = dim_ci_stage.dim_ci_stage_id
    INNER JOIN dim_ci_pipeline
      ON ci_runner_activity_monthly.dim_ci_pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
    INNER JOIN dim_project
      ON ci_runner_activity_monthly.dim_project_id = dim_project.dim_project_id
    INNER JOIN dim_namespace
      ON ci_runner_activity_monthly.dim_namespace_id = dim_namespace.dim_namespace_id
    INNER JOIN dim_user
      ON ci_runner_activity_monthly.dim_user_id = dim_user.dim_user_id
    INNER JOIN dim_date
      ON ci_runner_activity_monthly.created_date_id = dim_date.date_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-07-26",
    updated_date="2021-07-26"
) }}