{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_build_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date')

]) }}

, source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_stages_dedupe_source') }}
    WHERE created_at IS NOT NULL

), renamed AS (
  
    SELECT
      id::NUMBER            AS dim_ci_stage_id,
      project_id::NUMBER    AS dim_project_id,
      pipeline_id::NUMBER   AS dim_pipeline_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      name::VARCHAR         AS ci_stage_name,
      status::NUMBER        AS ci_stage_status,
      lock_version::NUMBER  AS lock_version,
      position::NUMBER      AS position
    FROM source

), joined AS (

    SELECT 
      dim_ci_stage_id,
      dim_project_id,
      dim_pipeline_id,
      date_day AS created_date_id,
      created_at,
      updated_at,
      ci_stage_name,
      ci_stage_status,
      lock_version,
      position
    FROM renamed
    LEFT JOIN dim_date
      ON TO_DATE(renamed.created_at) = dim_date.date_day
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-29",
    updated_date="2021-06-29"
) }}

