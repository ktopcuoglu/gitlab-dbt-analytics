WITH prep_merge_request AS (

    SELECT 
      dim_merge_request_id,
          
      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      created_date_id,
      dim_plan_id,
      author_id,
      milestone_id,
      assignee_id,
      merge_user_id,
      updated_by_id,
      last_edited_by_id,
      head_ci_pipeline_id,

      merge_request_internal_id,
      -- merge_request_title, sensitive masked
      is_merge_to_master,
      merge_error,
      latest_merge_request_diff_id,
      approvals_before_merge,
      lock_version,
      time_estimate,
      project_id,
      merge_request_state_id,
      merge_request_state,
      merge_request_status,
      does_merge_when_pipeline_succeeds,
      does_squash,
      is_discussion_locked,
      does_allow_maintainer_to_push,
      created_at,
      updated_at,
      merge_request_last_edited_at
    FROM {{ ref('prep_merge_request') }}

)

{{ dbt_audit(
    cte_ref="prep_merge_request",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-17",
    updated_date="2021-06-17"
) }}

