WITH prep_issue AS (

    SELECT 
      dim_issue_id,
            
      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_epic_id,
      created_date_id,
      dim_plan_id,
      author_id,
      milestone_id,
      sprint_id,

      issue_internal_id,
      updated_by_id,
      last_edited_by_id,
      moved_to_id,
      created_at,
      updated_at,
      issue_last_edited_at,
      issue_closed_at,
      is_confidential,
      issue_title,
      issue_description,

      weight,
      due_date,
      lock_version,
      time_estimate,
      has_discussion_locked,
      closed_by_id,
      relative_position,
      service_desk_reply_to,
      state_id,
      state_name,
      duplicated_to_id,
      promoted_to_epic_id,
      issue_type,
      severity,
      issue_url,
      milestone_title,
      milestone_due_date,
      labels,
      upvote_count
    FROM {{ ref('prep_issue') }}

)

{{ dbt_audit(
    cte_ref="prep_issue",
    created_by="@mpeychet_",
    updated_by="@jpeguero",
    created_date="2021-06-17",
    updated_date="2021-10-24"
) }}

