-- These data models are required for this data model based on https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_dotcom/xf/gitlab_dotcom_merge_requests_xf.sql
-- This data model is missing a lot of other source data models
WITH merge_requests AS (

    SELECT 
      {{ dbt_utils.star(from=ref('gitlab_ops_merge_requests'), except=["created_at", "updated_at"]) }},
      created_at AS merge_request_created_at,
      updated_at  AS merge_request_updated_at
    FROM {{ref('gitlab_ops_merge_requests')}} merge_requests
    --  WHERE {{ filter_out_blocked_users('merge_requests', 'author_id') }} -- these ids are probably different 

), label_links AS (

    SELECT *
    FROM {{ref('gitlab_ops_label_links')}}
    WHERE is_currently_valid = True
      AND target_type = 'MergeRequest'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_ops_labels_xf')}}

), agg_labels AS (

    SELECT
      merge_requests.merge_request_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM merge_requests
    LEFT JOIN label_links
      ON merge_requests.merge_request_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY merge_requests.merge_request_id

), projects AS (

    SELECT *
    FROM {{ref('gitlab_ops_projects_xf')}}

), joined AS (

    SELECT
      merge_requests.*, 
      projects.namespace_id,
      projects.ultimate_parent_id,
      projects.ultimate_parent_plan_id,
      projects.ultimate_parent_plan_title,
      projects.ultimate_parent_plan_is_paid,
      projects.namespace_is_internal,
      ARRAY_TO_STRING(agg_labels.labels,'|')                  AS masked_label_title,
      agg_labels.labels,
      IFF(projects.namespace_is_internal IS NOT NULL
          AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.labels),
        TRUE, FALSE)                                          AS is_community_contributor_related
    FROM merge_requests
    LEFT JOIN agg_labels
      ON merge_requests.merge_request_id = agg_labels.merge_request_id
    LEFT JOIN projects
        ON merge_requests.target_project_id = projects.project_id

)

SELECT *
FROM joined
