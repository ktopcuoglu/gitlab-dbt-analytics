-- depends_on: {{ ref('projects_part_of_product_ops') }}
-- These data models are required for this data model based on https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_dotcom/xf/gitlab_dotcom_merge_requests_xf.sql
-- This data model is missing a lot of other source data models
WITH merge_requests AS (

    SELECT 
      {{ dbt_utils.star(from=ref('gitlab_ops_merge_requests'), except=["created_at", "updated_at"]) }},
      created_at                                                                           AS merge_request_created_at,
      updated_at                                                                           AS merge_request_updated_at
    FROM {{ref('gitlab_ops_merge_requests')}} merge_requests

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
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC)  AS labels
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
      ARRAY_TO_STRING(agg_labels.labels,'|')                                               AS masked_label_title,
      agg_labels.labels, 
      {% set ops_projects = is_project_part_of_product_ops() %}
      {% if ops_projects|length > 0 %}
        IFF(merge_requests.target_project_id IN ({{ops_projects}}), TRUE, FALSE)           AS is_part_of_product_ops
      {% else %}
        FALSE                                                                              AS is_part_of_product_ops
      {% endif %}
    FROM merge_requests
    LEFT JOIN agg_labels
      ON merge_requests.merge_request_id = agg_labels.merge_request_id
    LEFT JOIN projects
        ON merge_requests.target_project_id = projects.project_id

)






SELECT *
FROM joined
