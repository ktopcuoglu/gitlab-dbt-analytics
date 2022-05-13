{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_resource_weight_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
    ('dim_issue', 'dim_issue')
]) }}

, resource_weight_events AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_weight_events_source') }} 
    {% if is_incremental() %}

    WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT 
      resource_weight_event_id                                      AS dim_resource_weight_id,
      resource_weight_events.user_id                                AS dim_user_id,
      resource_weight_events.created_at,
      dim_date.date_id                                              AS created_date_id,
      IFNULL(dim_project.dim_project_id, -1)                        AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)          AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)               AS dim_plan_id
    FROM resource_weight_events
    LEFT JOIN dim_issue
      ON resource_weight_events.issue_id = dim_issue.dim_issue_id
    LEFT JOIN dim_project
      ON dim_issue.dim_project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist 
      ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND  resource_weight_events.created_at >= dim_namespace_plan_hist.valid_from
      AND  resource_weight_events.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date ON TO_DATE(resource_weight_events.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-04-01",
    updated_date="2022-04-01"
) }}