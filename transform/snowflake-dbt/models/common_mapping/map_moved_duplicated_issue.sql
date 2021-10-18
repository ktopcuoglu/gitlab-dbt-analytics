WITH RECURSIVE issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}

), issues_moved_duplicated AS (

    SELECT
      *,
      IFNULL(moved_to_id, duplicated_to_id) AS moved_duplicated_to_id
    FROM issues

), recursive_cte(issue_id, moved_duplicated_to_id, issue_lineage) AS (

    SELECT
      issue_id,
      moved_duplicated_to_id,
      TO_ARRAY(issue_id) AS issue_lineage
    FROM issues_moved_duplicated
    WHERE moved_duplicated_to_id IS NULL

    UNION ALL

    SELECT
      iter.issue_id,
      iter.moved_duplicated_to_id,
      ARRAY_INSERT(anchor.issue_lineage, 0, iter.issue_id) AS issue_lineage
    FROM recursive_cte AS anchor
    INNER JOIN issues_moved_duplicated AS iter
      ON iter.moved_duplicated_to_id = anchor.issue_id

), final AS (

    SELECT
      issue_id                                                             AS issue_id,
      issue_lineage                                                        AS issue_lineage,
      issue_lineage[ARRAY_SIZE(issue_lineage) - 1]::NUMBER                 AS last_moved_duplicated_issue_id,
      IFF(last_moved_duplicated_issue_id != issue_id, TRUE, FALSE)         AS is_issue_moved_duplicated,
      --return final common dimension mapping,
      last_moved_duplicated_issue_id                                       AS dim_issue_id
    FROM recursive_cte

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-10-12",
    updated_date="2021-10-12",
) }}
