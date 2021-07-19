{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_stage_id"
    })
}}

{{ simple_cte([
    ('dim_project', 'dim_project'),
    ('dim_ci_pipeline', 'dim_ci_pipeline'),
    ('dim_date', 'dim_date')
]) }}

, ci_stages AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_stages_dedupe_source') }}
    WHERE created_at IS NOT NULL

), joined AS (
  
    SELECT
      ci_stages.id::NUMBER                              AS dim_ci_stage_id,
      IFNULL(dim_project.dim_project_id, -1)            AS dim_project_id,
      IFNULL(dim_ci_pipeline.dim_ci_pipeline_id, -1)    AS dim_ci_pipeline_id,
      IFNULL(dim_date.date_id, -1)                      AS created_date_id,
      ci_stages.created_at::TIMESTAMP                   AS created_at,
      ci_stages.updated_at::TIMESTAMP                   AS updated_at,
      ci_stages.name::VARCHAR                           AS ci_stage_name,
      ci_stages.status::NUMBER                          AS ci_stage_status,
      ci_stages.lock_version::NUMBER                    AS lock_version,
      ci_stages.position::NUMBER                        AS position
    FROM ci_stages
    LEFT JOIN dim_project
      ON ci_stages.project_id = dim_project.dim_project_id
    LEFT JOIN dim_ci_pipeline
      ON ci_stages.pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
    LEFT JOIN dim_date
      ON TO_DATE(ci_stages.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-06-29",
    updated_date="2021-07-15"
) }}
