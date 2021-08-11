{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('ci_runner_activity', 'fct_ci_runner_activity'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_date', 'dim_date')
]) }}

, month_spine AS (

    SELECT DISTINCT
      ci_runner_activity.dim_ci_build_id,
      ci_runner_activity.ci_build_started_at,
      ci_runner_activity.ci_build_finished_at,
      dim_date.first_day_of_month
    FROM ci_runner_activity
    INNER JOIN dim_date
      ON TO_DATE(ci_runner_activity.ci_build_started_at) <= dim_date.date_day
      AND ci_runner_activity.ci_build_finished_at > dim_date.date_day

), multi_month_ci_builds AS (

    SELECT
      dim_ci_build_id,
      first_day_of_month,
      LEAD(first_day_of_month, 1, '9999-12-31')
        OVER (PARTITION BY dim_ci_build_id ORDER BY first_day_of_month) AS last_day_of_month,
      DATEDIFF('seconds',
                IFF(ci_build_started_at > first_day_of_month,
                    ci_build_started_at,  first_day_of_month),
                IFF(ci_build_finished_at < last_day_of_month,
                    ci_build_finished_at,  last_day_of_month)
              )                                                         AS monthly_duration_in_s
    FROM month_spine

), ci_runner_activity_monthly AS (

    SELECT
      multi_month_ci_builds.first_day_of_month                          AS report_month,
      ci_runner_activity.dim_namespace_id,
      ci_runner_activity.is_paid_by_gitlab,
      ci_runner_activity.public_projects_minutes_cost_factor,
      ci_runner_activity.private_projects_minutes_cost_factor,
      SUM(multi_month_ci_builds.monthly_duration_in_s)                  AS ci_build_duration_in_s
    FROM ci_runner_activity
    INNER JOIN multi_month_ci_builds
      ON ci_runner_activity.dim_ci_build_id = multi_month_ci_builds.dim_ci_build_id
    {{ dbt_utils.group_by(n=5) }}

), joined AS (

    SELECT
      ci_runner_activity_monthly.report_month,
      dim_namespace.dim_namespace_id,
      dim_namespace.ultimate_parent_namespace_id,
      dim_namespace.gitlab_plan_id                                      AS dim_plan_id,
      

      -- CI RUNNER METRICS
      ci_runner_activity_monthly.ci_build_duration_in_s,
      ci_runner_activity_monthly.public_projects_minutes_cost_factor,
      ci_runner_activity_monthly.private_projects_minutes_cost_factor,

      -- CI RUNNER ACTIVITY METADATA
      ci_runner_activity_monthly.is_paid_by_gitlab,
      dim_namespace.namespace_is_internal,
      dim_namespace.gitlab_plan_title                                   AS ultimate_parent_plan_title

    FROM ci_runner_activity_monthly
    INNER JOIN dim_namespace
      ON ci_runner_activity_monthly.dim_namespace_id = dim_namespace.dim_namespace_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@snalamaru",
    created_date="2021-07-30",
    updated_date="2021-08-02"
) }}