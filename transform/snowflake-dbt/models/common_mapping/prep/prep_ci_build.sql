{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('gitlab_dotcom_ci_buillds_source', 'gitlab_dotcom_ci_buillds_source'),
    ('dim_date', 'dim_date'),
]) }}

, renamed AS (
  
    SELECT
      ci_build_id AS dim_ci_build_id, 
      
      -- FOREIGN KEYS
      gitlab_dotcom_ci_buillds_source.ci_build_project_id     AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      prep_user.dim_user_id,
      dim_date.date_id                                        AS ci_build_creation_dim_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)         AS dim_plan_id,
      merge_request_id,
      ci_build_runner_id                                      AS ci_runner_id
      ci_build_user_id                                        AS dim_usuer_id,
      ci_build_stage_id                                       AS dim_ci_stage_id,

      status,
      finished_at,
      trace,
      created_at,
      updated_at,
      started_at,
      coverage,
      ci_build_commit_id AS commit_id,
      ci_build_name AS name,
      options,
      allow_failure,
      stage,
      ci_build_trigger_request_id AS trigger_request_id,
      stage_idx,
      tag,
      ref,
      type,
      target_url,
      description,
      ci_build_erased_by_id AS erased_by_id,
      ci_build_erased_at AS erased_at,
      ci_build_artifacts_expire_at AS artifacts_expire_at,
      environment,
      yaml_variables,
      ci_build_queued_at AS queued_at,
      lock_version,
      coverage_regex,
      ci_build_auto_canceled_by_id AS auto_canceled_by_id,
      retried,
      protected,
      failure_reason,
      ci_build_scheduled_at AS scheduled_at,
      upstream_pipeline_id

    FROM gitlab_dotcom_ci_buillds_source
    LEFT JOIN prep_project ON gitlab_dotcom_ci_buillds_source.ci_build_project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_ci_buillds_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_ci_buillds_source.created_at < dim_namespace_plan_hist.valid_to
    LEFT JOIN prep_user ON gitlab_dotcom_ci_buillds_source.ci_build_user_id = prep_user.dim_user_id
    LEFT JOIN dim_date ON TO_DATE(gitlab_dotcom_ci_buillds_source.created_at) = dim_date.date_day
    WHERE gitlab_dotcom_ci_buillds_source.project_id IS NOT  NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-10",
    updated_date="2021-06-10"
) }}
