{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('gitlab_dotcom_ci_pipelines_dedupe_source', 'gitlab_dotcom_ci_pipelines_dedupe_source'),
    ('dim_date', 'dim_date'),
]) }}

, renamed AS (
  
    SELECT
      ci_pipeline_id AS dim_ci_pipeline_id, 
      
      -- FOREIGN KEYS
      gitlab_dotcom_events_source dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      prep_user.dim_user_id,
      dim_date.date_id                                  AS event_creation_dim_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)   AS dim_plan_id,
      merge_request_id,

      created_at, 
      started_at, 
      committed_at,
      finished_at, 
      ci_pipeline_duration, 

      status, 
      ref,
      has_tag, 
      yaml_errors, 
      lock_version, 
      auto_canceled_by_id, 
      pipeline_schedule_id, 
      ci_pipeline_source, 
      config_source, 
      is_protected, 
      failure_reason, 
      ci_pipeline_iid
    FROM gitlab_dotcom_ci_pipelines_dedupe_source
    LEFT JOIN prep_project ON gitlab_dotcom_ci_pipelines_dedupe_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_ci_pipelines_dedupe_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_ci_pipelines_dedupe_source.created_at < dim_namespace_plan_hist.valid_to
    LEFT JOIN prep_user ON gitlab_dotcom_ci_pipelines_dedupe_source.author_id = prep_user.dim_user_id
    LEFT JOIN dim_date ON TO_DATE(gitlab_dotcom_ci_pipelines_dedupe_source.created_at) = dim_date.date_day

)

SELECT * FROM renamed
