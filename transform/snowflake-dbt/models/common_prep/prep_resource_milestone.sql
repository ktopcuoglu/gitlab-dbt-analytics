{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_resource_milestone_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_issue', 'dim_issue'),
    ('dim_merge_request', 'dim_merge_request'),
]) }}

, resource_milestone_events AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_milestone_events_source') }} 
    {% if is_incremental() %}

    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

) , joined AS (

    SELECT
      resource_milestone_events.resource_milestone_event_id                                 AS dim_resource_milestone_id,
      COALESCE(dim_issue.dim_project_id, 
                dim_merge_request.dim_project_id)                                           AS dim_project_id,
      COALESCE(dim_issue.dim_plan_id,
                dim_merge_request.dim_plan_id)                                              AS dim_plan_id,
      COALESCE(dim_issue.ultimate_parent_namespace_id,
                dim_merge_request.ultimate_parent_namespace_id)                             AS ultimate_parent_namespace_id,
      user_id                                                                               AS dim_user_id,
      issue_id,
      merge_request_id,
      resource_milestone_events.created_at,
      dim_date.date_id                                                                      AS created_date_id
    FROM resource_milestone_events
    LEFT JOIN dim_issue
      ON resource_milestone_events.issue_id = dim_issue.dim_issue_id
    LEFT JOIN dim_merge_request
      ON resource_milestone_events.merge_request_id = dim_merge_request.dim_merge_request_id
    LEFT JOIN dim_date 
      ON TO_DATE(resource_milestone_events.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-23",
    updated_date="2022-03-23"
) }}