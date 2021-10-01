{{config({
   "schema": "common_mart_product"
 })
}}
 
{{ simple_cte([
   ('dim_namespace', 'dim_namespace'),
   ('dim_issue', 'dim_issue'),
   ('dim_project', 'dim_project'),
   ('dim_date', 'dim_date'),
   ('fct_issue_metrics', 'fct_issue_metrics')
]) }}
 
, base AS (
 
    SELECT
      -- unique identifers for date & issue Id
      fct_issue_metrics.date_id,
      fct_issue_metrics.date_actual,
      fct_issue_metrics.dim_issue_id,
      fct_issue_metrics.severity,
      fct_issue_metrics.labels,
      fct_issue_metrics.open_age_in_days,
      fct_issue_metrics.open_age_after_sev_label_added,

      dim_issue.issue_title,
      dim_issue.created_at                    AS issue_created_at
      dim_issue.issue_closed_at
      dim_issue.issue_internal_id,
      dim_issue.dim_namespace_id,
      dim_issue.ultimate_parent_namespace_id,
      dim_issue.dim_project_id,
      CASE 
        WHEN fct_issue_metrics.date_actual > dim_issue.issue_closed_at THEN 'closed'
        ELSE 'open'
      END AS issue_state,
      CASE 
        WHEN array_size(ARRAY_AGG(dim_user.username) WITHIN GROUP (ORDER BY dim_user.username)) = 0 THEN 1
        ELSE 0
      END AS issue_is_unassigned,
      ARRAY_AGG(DISTINCT dim_user.username) WITHIN GROUP (ORDER BY dim_user.username) AS assigned_usernames,
      -- build URL with namespace lineage of paths_
      CASE
        WHEN namespace_4 IS NOT NULL THEN namespace_4.namespace_path || '/' || namespace_3.namespace_path || '/' || namespace_2.namespace_path || '/' || namespace_1.namespace_path || '/' || namespace.namespace_path
        WHEN namespace_3 IS NOT NULL THEN namespace_3.namespace_path || '/' || namespace_2.namespace_path || '/' || namespace_1.namespace_path || '/' || namespace.namespace_path
        WHEN namespace_2 IS NOT NULL THEN namespace_2.namespace_path || '/' || namespace_1.namespace_path || '/' || namespace.namespace_path
        WHEN namespace_1 IS NOT NULL THEN namespace_1.namespace_path || '/' || namespace.namespace_path
        ELSE namespace.namespace_path
      END AS full_namespace_path,
      '[' || REPLACE(REPLACE(LEFT(dim_issue.issue_title,64),'[',''),']','') ||'](https://gitlab.com/' ||full_namespace_path || '/' || dim_project.project_path || '/issues/' || dim_issue.issue_internal_id||')' AS issue_url
   FROM fct_issue_metrics
    LEFT JOIN dim_issue
      ON fct_issue_metrics.dim_issue_id = dim_issue.dim_issue_id
    LEFT JOIN prep_issue_assignees
      ON dim_issue.dim_issue_id = prep_issue_assignees.dim_issue_id
    LEFT JOIN dim_project
      ON dim_issue.dim_project_id = dim_project.dim_project_id
    LEFT JOIN dim_user
      ON prep_issue_assignees.dim_user_id = dim_user.dim_user_id
    -- recursive join to get namespace paths for URL
    INNER JOIN dim_namespace namespace
      ON dim_issue.dim_namespace_id = dim_namespace.dim_namespace_id
    LEFT OUTER JOIN dim_namespace namespace_1
      ON namespace_1.dim_namespace_id = namespace.parent_id
      -- make sure haven't reached top level namespace yet
      AND namespace.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN dim_namespace namespace_2
      ON namespace_2.dim_namespace_id = namespace_1.parent_id
      AND namespace_1.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN dim_namespace namespace_3
      ON namespace_3.dim_namespace_id = namespace_2.parent_id
      AND namespace_2.namespace_is_ultimate_parent = FALSE
    LEFT OUTER JOIN dim_namespace namespace_4
      ON namespace_4.dim_namespace_id = namespace_3.parent_id
      AND namespace_3.namespace_is_ultimate_parent = FALSE
    -- scope to only main gitlab projects for now
    WHERE dim_issue.ultimate_parent_namespace_id IN ('6543','9970')
 
 
), joined AS (
    SELECT * FROM base
)
 
{{ dbt_audit(
   cte_ref="joined",
   created_by="@dtownsend",
   updated_by="@dtownsend",
   created_date="2021-10-01",
   updated_date="2021-10-01"
) }}