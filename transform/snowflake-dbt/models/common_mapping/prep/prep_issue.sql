{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_issue_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user'),
    ('prep_issue_severity', 'prep_issue_severity'),
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels')
]) }}

, gitlab_dotcom_issues_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source')}}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (
  
    SELECT
      gitlab_dotcom_issues_source.issue_id                        AS dim_issue_id,
      
      -- FOREIGN KEYS
      gitlab_dotcom_issues_source.project_id                      AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      dim_date.date_id                                            AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)             AS dim_plan_id,
      gitlab_dotcom_issues_source.author_id,
      gitlab_dotcom_issues_source.milestone_id,
      gitlab_dotcom_issues_source.sprint_id,

      gitlab_dotcom_issues_source.issue_iid                       AS issue_internal_id,
      gitlab_dotcom_issues_source.updated_by_id,
      gitlab_dotcom_issues_source.last_edited_by_id,
      gitlab_dotcom_issues_source.moved_to_id,
      gitlab_dotcom_issues_source.created_at,
      gitlab_dotcom_issues_source.updated_at,
      gitlab_dotcom_issues_source.issue_last_edited_at,
      gitlab_dotcom_issues_source.issue_closed_at,
      gitlab_dotcom_issues_source.is_confidential,
      gitlab_dotcom_issues_source.issue_title,
      gitlab_dotcom_issues_source.issue_description,

      gitlab_dotcom_issues_source.weight,
      gitlab_dotcom_issues_source.due_date,
      gitlab_dotcom_issues_source.lock_version,
      gitlab_dotcom_issues_source.time_estimate,
      gitlab_dotcom_issues_source.has_discussion_locked,
      gitlab_dotcom_issues_source.closed_by_id,
      gitlab_dotcom_issues_source.relative_position,
      gitlab_dotcom_issues_source.service_desk_reply_to,
      gitlab_dotcom_issues_source.state_id,
        {{ map_state_id('state_id') }}                            AS state_name,
      gitlab_dotcom_issues_source.duplicated_to_id,
      gitlab_dotcom_issues_source.promoted_to_epic_id,
      gitlab_dotcom_issues_source.issue_type, 
      -- get issue severity from prep_issue_severity & prep_labels
      -- if issue is GitLab Incident, uses built in Severity label, otherwise try to pull from issue labels
      CASE 
        WHEN prep_issue_severity.severity = 4 THEN 'Severity 1'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%1%' THEN 'Severity 1'
        WHEN prep_issue_severity.severity = 3 THEN 'Severity 2'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%2%' THEN 'Severity 2'
        WHEN prep_issue_severity.severity = 2 THEN 'Severity 3'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%3%' THEN 'Severity 3'
        WHEN prep_issue_severity.severity = 1 THEN 'Severity 4'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%4%' THEN 'Severity 4'
        ELSE NULL
      END AS severity
    FROM gitlab_dotcom_issues_source
    LEFT JOIN prep_project 
      ON gitlab_dotcom_issues_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist 
      ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_issues_source.created_at >= dim_namespace_plan_hist.valid_from
      AND gitlab_dotcom_issues_source.created_at < dim_namespace_plan_hist.valid_to
    LEFT JOIN dim_date 
      ON TO_DATE(gitlab_dotcom_issues_source.created_at) = dim_date.date_day
    LEFT JOIN prep_issue_severity
      ON gitlab_dotcom_issues_source.issue_id = prep_issue_severity.dim_issue_id
    LEFT JOIN prep_label_links
      ON gitlab_dotcom_issues_source.issue_id = prep_label_links.dim_issue_severity_id
    LEFT JOIN prep_labels 
      ON prep_label_links.dim_label_id = prep_labels.dim_label_id
    WHERE gitlab_dotcom_issues_source.project_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-17",
    updated_date="2021-06-17"
) }}
