{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_job_artifact_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

,  ci_job_artifacts AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_job_artifacts_source')}}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}


), joined AS (
    
    SELECT
      ci_job_artifact_id                                    AS dim_ci_job_artifact_id,
      project_id                                            AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)  AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)       AS dim_plan_id,
      file_type,
      ci_job_artifacts.created_at,
      ci_job_artifacts.updated_at,
      dim_date.date_id                                      AS created_date_id
    FROM ci_job_artifacts
    LEFT JOIN dim_project 
      ON ci_job_artifacts.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND ci_job_artifacts.created_at >= dim_namespace_plan_hist.valid_from
        AND ci_job_artifacts.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date 
      ON TO_DATE(ci_job_artifacts.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-24",
    updated_date="2022-03-24"
) }}
