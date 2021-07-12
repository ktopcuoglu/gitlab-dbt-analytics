WITh prep AS (

    SELECT *  FROM {{ ref('prep_ci_pipeline')}}

), final AS (

    SELECT 
      dim_ci_pipeline_id, 

      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_user_id,
      created_date_id,
      dim_plan_id,
      merge_request_id,

      created_at, 
      started_at, 
      committed_at,
      finished_at, 
      ci_pipeline_duration_in_s, 

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
      failure_reason_id,
      failure_reason,
      ci_pipeline_internal_id
    FROM prep

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-10",
    updated_date="2021-06-10"
) }}
