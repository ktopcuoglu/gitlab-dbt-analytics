WITH ci_stages AS (
  
    SELECT
      --PRIMARY KEY
      dim_ci_stage_id,

      -- FOREIGN KEYS
      dim_project_id,
      dim_ci_pipeline_id,
      created_date_id,

      -- metadata
      created_at,
      updated_at,
      ci_stage_name,
      ci_stage_status,
      lock_version,
      position
    FROM {{ ref('prep_ci_stage') }}

)

{{ dbt_audit(
    cte_ref="ci_stages",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-06-29",
    updated_date="2021-07-08"
) }}

