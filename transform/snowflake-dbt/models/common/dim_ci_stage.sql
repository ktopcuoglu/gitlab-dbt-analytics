WITH source AS (

  SELECT *
  FROM {{ ref('prep_ci_stage') }}

), renamed AS (
  
    SELECT
      dim_ci_stage_id,

      -- FOREIGN KEY
      dim_project_id,
      dim_pipeline_id,
      created_date_id,

      -- metadata
      created_at,
      updated_at,
      ci_stage_status,
      lock_version,
      position
    FROM source

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-29",
    updated_date="2021-06-29"
) }}

