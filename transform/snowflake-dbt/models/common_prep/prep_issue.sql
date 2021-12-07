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
    ('prep_labels', 'prep_labels'),
    ('gitlab_dotcom_epic_issues_source', 'gitlab_dotcom_epic_issues_source'),
    ('gitlab_dotcom_routes_source', 'gitlab_dotcom_routes_source'),
    ('gitlab_dotcom_projects_source', 'gitlab_dotcom_projects_source'),
    ('gitlab_dotcom_milestones_source', 'gitlab_dotcom_milestones_source'),
    ('gitlab_dotcom_award_emoji_source', 'gitlab_dotcom_award_emoji_source')
]) }}

, gitlab_dotcom_issues_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source')}}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), upvote_count AS (

    SELECT
      awardable_id                                        AS dim_issue_id,
      SUM(IFF(award_emoji_name LIKE 'thumbsup%', 1, 0))   AS thumbsups_count,
      SUM(IFF(award_emoji_name LIKE 'thumbsdown%', 1, 0)) AS thumbsdowns_count,
      thumbsups_count - thumbsdowns_count                 AS upvote_count
    FROM gitlab_dotcom_award_emoji_source
    WHERE awardable_type = 'Issue'
    GROUP BY 1

), agg_labels AS (

    SELECT 
      gitlab_dotcom_issues_source.issue_id                                                          AS dim_issue_id,
      ARRAY_AGG(LOWER(prep_labels.label_title)) WITHIN GROUP (ORDER BY prep_labels.label_title ASC) AS labels
    FROM gitlab_dotcom_issues_source
    LEFT JOIN prep_label_links
        ON gitlab_dotcom_issues_source.issue_id = prep_label_links.dim_issue_id
    LEFT JOIN prep_labels
        ON prep_label_links.dim_label_id = prep_labels.dim_label_id
    GROUP BY gitlab_dotcom_issues_source.issue_id  


), renamed AS (
  
    SELECT
      gitlab_dotcom_issues_source.issue_id                        AS dim_issue_id,
      
      -- FOREIGN KEYS
      gitlab_dotcom_issues_source.project_id                      AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      gitlab_dotcom_epic_issues_source.epic_id                    AS dim_epic_id,
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
      CASE 
        WHEN prep_issue_severity.severity = 4 THEN 'S1'
        WHEN ARRAY_CONTAINS('severity::1'::variant, agg_labels.labels) OR ARRAY_CONTAINS('s1'::variant, agg_labels.labels) THEN 'S1'
        WHEN prep_issue_severity.severity = 3 THEN 'S2'
        WHEN ARRAY_CONTAINS('severity::2'::variant, agg_labels.labels) OR ARRAY_CONTAINS('s2'::variant, agg_labels.labels) THEN 'S2'
        WHEN prep_issue_severity.severity = 2 THEN 'S3'
        WHEN ARRAY_CONTAINS('severity::3'::variant, agg_labels.labels) OR ARRAY_CONTAINS('s3'::variant, agg_labels.labels) THEN 'S3'
        WHEN prep_issue_severity.severity = 1 THEN 'S4'
        WHEN ARRAY_CONTAINS('severity::4'::variant, agg_labels.labels) OR ARRAY_CONTAINS('s4'::variant, agg_labels.labels) THEN 'S4'
        ELSE NULL
      END AS severity,
      IFF(gitlab_dotcom_projects_source.visibility_level = 'private',
        'private - masked',
        'https://gitlab.com/' || gitlab_dotcom_routes_source.path || '/issues/' || gitlab_dotcom_issues_source.issue_iid)
         AS issue_url,
      IFF(gitlab_dotcom_projects_source.visibility_level = 'private',
        'private - masked',
        gitlab_dotcom_milestones_source.milestone_title)    AS milestone_title,
      gitlab_dotcom_milestones_source.due_date              AS milestone_due_date,
      agg_labels.labels,
      IFNULL(upvote_count.upvote_count, 0)                  AS upvote_count
    FROM gitlab_dotcom_issues_source
    LEFT JOIN agg_labels
        ON gitlab_dotcom_issues_source.issue_id = agg_labels.dim_issue_id
    LEFT JOIN prep_project 
      ON gitlab_dotcom_issues_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist 
      ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_issues_source.created_at >= dim_namespace_plan_hist.valid_from
      AND gitlab_dotcom_issues_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date 
      ON TO_DATE(gitlab_dotcom_issues_source.created_at) = dim_date.date_day
    LEFT JOIN prep_issue_severity
      ON gitlab_dotcom_issues_source.issue_id = prep_issue_severity.dim_issue_id
    LEFT JOIN gitlab_dotcom_epic_issues_source
      ON gitlab_dotcom_issues_source.issue_id = gitlab_dotcom_epic_issues_source.issue_id
    LEFT JOIN gitlab_dotcom_projects_source
      ON gitlab_dotcom_projects_source.project_id = gitlab_dotcom_issues_source.project_id
    LEFT JOIN gitlab_dotcom_routes_source
      ON gitlab_dotcom_routes_source.source_id = gitlab_dotcom_issues_source.project_id
      AND gitlab_dotcom_routes_source.source_type = 'Project'
    LEFT JOIN gitlab_dotcom_milestones_source
      ON gitlab_dotcom_milestones_source.milestone_id = gitlab_dotcom_issues_source.milestone_id
    LEFT JOIN upvote_count
      ON upvote_count.dim_issue_id = gitlab_dotcom_issues_source.issue_id
    WHERE gitlab_dotcom_issues_source.project_id IS NOT NULL
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@jpeguero",
    created_date="2021-06-17",
    updated_date="2021-10-24"
) }}
