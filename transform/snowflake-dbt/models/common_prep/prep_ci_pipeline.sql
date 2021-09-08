{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('gitlab_dotcom_ci_pipelines_source', 'gitlab_dotcom_ci_pipelines_source'),
    ('dim_date', 'dim_date'),
]) }}

, renamed AS (
  
    SELECT
      ci_pipeline_id                                            AS dim_ci_pipeline_id, 
      
      -- FOREIGN KEYS
      gitlab_dotcom_ci_pipelines_source.project_id              AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      prep_user.dim_user_id,
      dim_date.date_id                                          AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)           AS dim_plan_id,
      merge_request_id,

      gitlab_dotcom_ci_pipelines_source.created_at, 
      gitlab_dotcom_ci_pipelines_source.started_at, 
      gitlab_dotcom_ci_pipelines_source.committed_at,
      gitlab_dotcom_ci_pipelines_source.finished_at, 
      gitlab_dotcom_ci_pipelines_source.ci_pipeline_duration    AS ci_pipeline_duration_in_s, 

      gitlab_dotcom_ci_pipelines_source.status, 
      gitlab_dotcom_ci_pipelines_source.ref,
      gitlab_dotcom_ci_pipelines_source.has_tag, 
      gitlab_dotcom_ci_pipelines_source.yaml_errors, 
      gitlab_dotcom_ci_pipelines_source.lock_version, 
      gitlab_dotcom_ci_pipelines_source.auto_canceled_by_id, 
      gitlab_dotcom_ci_pipelines_source.pipeline_schedule_id, 
      gitlab_dotcom_ci_pipelines_source.ci_pipeline_source, 
      gitlab_dotcom_ci_pipelines_source.config_source, 
      gitlab_dotcom_ci_pipelines_source.is_protected, 
      gitlab_dotcom_ci_pipelines_source.failure_reason          AS failure_reason_id,
      {{ map_ci_pipeline_failure_reason('failure_reason_id') }} AS failure_reason,
      gitlab_dotcom_ci_pipelines_source.ci_pipeline_iid         AS ci_pipeline_internal_id
    FROM gitlab_dotcom_ci_pipelines_source
    LEFT JOIN prep_project ON gitlab_dotcom_ci_pipelines_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_ci_pipelines_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_ci_pipelines_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_user ON gitlab_dotcom_ci_pipelines_source.user_id = prep_user.dim_user_id
    LEFT JOIN dim_date ON TO_DATE(gitlab_dotcom_ci_pipelines_source.created_at) = dim_date.date_day
    WHERE gitlab_dotcom_ci_pipelines_source.project_id IS NOT  NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-10",
    updated_date="2021-06-10"
) }}
