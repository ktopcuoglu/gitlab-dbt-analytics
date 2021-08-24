{{config({
   "schema": "common_mart_product"
 })
}}
 
{{ simple_cte([
   ('ci_runner_activity', 'fct_ci_runner_activity'),
   ('dim_namespace', 'dim_namespace'),
   ('dim_project', 'dim_project'),
   ('dim_date', 'dim_date')
]) }}
 
, day_spine AS (
 
   SELECT DISTINCT
     ci_runner_activity.dim_ci_build_id,
     ci_runner_activity.ci_build_started_at,
     ci_runner_activity.ci_build_finished_at,
     dim_date.date_day
   FROM ci_runner_activity
   INNER JOIN dim_date
     ON TO_DATE(ci_runner_activity.ci_build_started_at) = dim_date.date_day
   WHERE ci_runner_activity.ci_build_started_at >= '2020-01-01'
 
), multi_day_ci_builds AS (
 
   SELECT
     dim_ci_build_id,
     date_day,
     LEAD(date_day, 1, '9999-12-31')
       OVER(PARTITION BY dim_ci_build_id ORDER BY date_day)            AS next_day,
     DATEDIFF('second',
               IFF(ci_build_started_at > date_day,
                   ci_build_started_at,  date_day),
               IFF(ci_build_finished_at < next_day,
                   ci_build_finished_at,  next_day)
             )                                                         AS daily_duration_in_s
   FROM day_spine
 
), ci_runner_activity_daily AS (
 
   SELECT
     multi_day_ci_builds.date_day                                      AS report_date,
     ci_runner_activity.dim_project_id,
     ci_runner_activity.dim_namespace_id,
     ci_runner_activity.is_paid_by_gitlab,
     ci_runner_activity.public_projects_minutes_cost_factor,
     ci_runner_activity.private_projects_minutes_cost_factor,
     SUM(multi_day_ci_builds.daily_duration_in_s)                      AS ci_build_duration_in_s
   FROM ci_runner_activity
   INNER JOIN multi_day_ci_builds
     ON ci_runner_activity.dim_ci_build_id = multi_day_ci_builds.dim_ci_build_id
   {{ dbt_utils.group_by(n=6) }}
 
), joined AS (
 
   SELECT
     ci_runner_activity_daily.report_date,
     dim_project.dim_project_id,
     dim_namespace.dim_namespace_id,
     dim_namespace.ultimate_parent_namespace_id,
     dim_namespace.gitlab_plan_id                                      AS dim_plan_id,
    
 
     -- CI RUNNER METRICS
     ci_runner_activity_daily.ci_build_duration_in_s,
     ci_runner_activity_daily.public_projects_minutes_cost_factor,
     ci_runner_activity_daily.private_projects_minutes_cost_factor,
 
     -- CI RUNNER ACTIVITY METADATA
     ci_runner_activity_daily.is_paid_by_gitlab,
     dim_project.visibility_level                                      AS project_visibility_level,
     dim_project.project_path,
     dim_namespace.namespace_is_internal,
     dim_namespace.gitlab_plan_title                                   AS ultimate_parent_plan_title
 
   FROM ci_runner_activity_daily
   INNER JOIN dim_project
     ON ci_runner_activity_daily.dim_project_id = dim_project.dim_project_id
   INNER JOIN dim_namespace
     ON ci_runner_activity_daily.dim_namespace_id = dim_namespace.dim_namespace_id
 
)
 
{{ dbt_audit(
   cte_ref="joined",
   created_by="@ischweickartDD",
   updated_by="@snalamaru",
   created_date="2021-07-30",
   updated_date="2021-08-24"
) }}
