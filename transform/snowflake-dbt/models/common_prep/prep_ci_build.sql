{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_build_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user')
]) }}

, gitlab_dotcom_ci_builds_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_builds_source')}}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (
  
    SELECT
      ci_build_id AS dim_ci_build_id, 
      
      -- FOREIGN KEYS
      gitlab_dotcom_ci_builds_source.ci_build_project_id          AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      dim_date.date_id                                            AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)             AS dim_plan_id,
      ci_build_runner_id                                          AS dim_ci_runner_id,
      ci_build_user_id                                            AS dim_user_id,
      ci_build_stage_id                                           AS dim_ci_stage_id,

      prep_project.namespace_is_internal,
      gitlab_dotcom_ci_builds_source.status                       AS ci_build_status,
      gitlab_dotcom_ci_builds_source.finished_at,
      gitlab_dotcom_ci_builds_source.trace,
      gitlab_dotcom_ci_builds_source.created_at,
      gitlab_dotcom_ci_builds_source.updated_at,
      gitlab_dotcom_ci_builds_source.started_at,
      gitlab_dotcom_ci_builds_source.coverage,
      gitlab_dotcom_ci_builds_source.ci_build_commit_id           AS commit_id,
      gitlab_dotcom_ci_builds_source.ci_build_name,
      gitlab_dotcom_ci_builds_source.options,
      gitlab_dotcom_ci_builds_source.allow_failure,
      gitlab_dotcom_ci_builds_source.stage,
      gitlab_dotcom_ci_builds_source.ci_build_trigger_request_id  AS trigger_request_id,
      gitlab_dotcom_ci_builds_source.stage_idx,
      gitlab_dotcom_ci_builds_source.tag,
      gitlab_dotcom_ci_builds_source.ref,
      gitlab_dotcom_ci_builds_source.type                         AS ci_build_type,
      gitlab_dotcom_ci_builds_source.target_url,
      gitlab_dotcom_ci_builds_source.description                  AS ci_build_description,
      gitlab_dotcom_ci_builds_source.ci_build_erased_by_id        AS erased_by_id,
      gitlab_dotcom_ci_builds_source.ci_build_erased_at           AS erased_at,
      gitlab_dotcom_ci_builds_source.ci_build_artifacts_expire_at AS artifacts_expire_at,
      gitlab_dotcom_ci_builds_source.environment,
      gitlab_dotcom_ci_builds_source.yaml_variables,
      gitlab_dotcom_ci_builds_source.ci_build_queued_at           AS queued_at,
      gitlab_dotcom_ci_builds_source.lock_version,
      gitlab_dotcom_ci_builds_source.coverage_regex,
      gitlab_dotcom_ci_builds_source.ci_build_auto_canceled_by_id AS auto_canceled_by_id,
      gitlab_dotcom_ci_builds_source.retried,
      gitlab_dotcom_ci_builds_source.protected,
      gitlab_dotcom_ci_builds_source.failure_reason,
      gitlab_dotcom_ci_builds_source.ci_build_scheduled_at        AS scheduled_at,
      gitlab_dotcom_ci_builds_source.upstream_pipeline_id,
      CASE
      WHEN ci_build_name LIKE '%apifuzzer_fuzz%' 
        THEN 'api_fuzzing'
      WHEN ci_build_name LIKE '%container_scanning%' 
        THEN 'container_scanning'
      WHEN ci_build_name LIKE '%dast%'  
        THEN 'dast' 
      WHEN ci_build_name LIKE '%dependency_scanning%'  
        THEN 'dependency_scanning'
      WHEN ci_build_name LIKE '%license_management%'  
        THEN 'license_management'
      WHEN ci_build_name LIKE '%license_scanning%'  
        THEN 'license_scanning'
      WHEN ci_build_name LIKE '%sast%'  
        THEN 'sast'  
      WHEN ci_build_name LIKE '%secret_detection%'
        THEN 'secret_detection'
      END                                                         AS secure_ci_build_type

    FROM gitlab_dotcom_ci_builds_source
    LEFT JOIN prep_project 
      ON gitlab_dotcom_ci_builds_source.ci_build_project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist 
      ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_ci_builds_source.created_at >= dim_namespace_plan_hist.valid_from
      AND gitlab_dotcom_ci_builds_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_user 
      ON gitlab_dotcom_ci_builds_source.ci_build_user_id = prep_user.dim_user_id
    LEFT JOIN dim_date 
      ON TO_DATE(gitlab_dotcom_ci_builds_source.created_at) = dim_date.date_day
    WHERE gitlab_dotcom_ci_builds_source.ci_build_project_id IS NOT  NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-06-17",
    updated_date="2021-07-09"
) }}
