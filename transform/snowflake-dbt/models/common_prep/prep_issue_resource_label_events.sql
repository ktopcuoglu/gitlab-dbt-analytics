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
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('resource_label_events', 'gitlab_dotcom_resource_label_events_source'),
    ('prep_epic', 'prep_epic'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request')
]) }}

, joined AS (

    SELECT 
      resource_label_events.resource_label_event_id                                     AS dim_issue_label_id,
      COALESCE(prep_epic.group_id,
                prep_issue.ultimate_parent_namespace_id,
                prep_merge_request.ultimate_parent_namespace_id)                        AS ultimate_parent_namespace_id,
      user_id                                                                           AS dim_user_id,
      
    FROM resource_label_events
    LEFT JOIN prep_epic
      ON resource_label_events.epic_id = prep_epic.dim_epic_id
    LEFT JOIN prep_issue
      ON resource_label_events.issue_id = prep_issue.dim_issue_id
    LEFT JOIN prep_merge_request
      ON resource_label_events.merge_request_id = mrs.dim_merge_request_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-14",
    updated_date="2022-03-14"
) }}
