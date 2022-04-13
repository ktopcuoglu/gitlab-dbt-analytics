{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_milestone_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
    ('dim_issue', 'dim_issue'),
    ('dim_epic', 'dim_epic')
]) }}

, milestones AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_milestones_source') }} 
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT 
      milestone_id                                                                      AS dim_milestone_id,
      milestones.created_at,
      milestones.updated_at,
      dim_date.date_id                                                                  AS created_date_id,
      IFNULL(dim_project.dim_project_id, -1)                                            AS dim_project_id,
      COALESCE(dim_project.ultimate_parent_namespace_id, milestones.group_id, -1)       AS ultimate_parent_namespace_id,
      COALESCE(dim_namespace_plan_hist.dim_plan_id, dim_epic.dim_plan_id, 34)           AS dim_plan_id
    FROM milestones
    LEFT JOIN dim_project
      ON milestones.project_id = dim_project.dim_project_id
    LEFT JOIN dim_epic
      ON milestones.group_id = dim_epic.dim_epic_id
    LEFT JOIN dim_namespace_plan_hist 
      ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND  milestones.created_at >= dim_namespace_plan_hist.valid_from
      AND  milestones.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date as dim_date ON TO_DATE(milestones.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-04-01",
    updated_date="2022-04-01"
) }}