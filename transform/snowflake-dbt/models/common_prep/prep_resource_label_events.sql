{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_issue_label_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_epic', 'prep_epic'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request'),
]) }}

, resource_label_events AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_label_events_source') }} 
    {% if is_incremental() %}

    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

) , joined AS (

    SELECT 
      resource_label_events.resource_label_event_id::NUMBER                 AS dim_issue_label_id,
      COALESCE(prep_issue.dim_project_id,
                prep_merge_request.dim_project_id)::NUMBER                  AS dim_project_id,
      COALESCE(prep_epic.dim_plan_id,
                prep_issue.dim_plan_id,
                prep_merge_request.dim_plan_id)::NUMBER                     AS dim_plan_id,
      COALESCE(prep_epic.group_id,
                prep_issue.ultimate_parent_namespace_id,
                prep_merge_request.ultimate_parent_namespace_id)::NUMBER    AS ultimate_parent_namespace_id,
      user_id::NUMBER                                                       AS dim_user_id,
      issue_id::NUMBER                                                      AS dim_issue_id,
      merge_request_id::NUMBER                                              AS dim_merge_request_id,
      epic_id::NUMBER                                                       AS dim_epic_id,
      resource_label_events.created_at::TIMESTAMP                           AS created_at,
      dim_date.date_id::NUMBER                                              AS created_date_id
    FROM resource_label_events
    LEFT JOIN prep_epic
      ON resource_label_events.epic_id = prep_epic.dim_epic_id
    LEFT JOIN prep_issue
      ON resource_label_events.issue_id = prep_issue.dim_issue_id
    LEFT JOIN prep_merge_request
      ON resource_label_events.merge_request_id = prep_merge_request.dim_merge_request_id
    LEFT JOIN dim_date 
      ON TO_DATE(resource_label_events.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-14",
    updated_date="2022-03-14"
) }}
