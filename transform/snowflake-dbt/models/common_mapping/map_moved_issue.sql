WITH RECURSIVE issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}

), recursive_cte(issue_id, moved_to_id, issue_lineage) AS (

    SELECT
      issue_id,
      moved_to_id,
      TO_ARRAY(issue_id) AS issue_lineage
    FROM issues
    WHERE moved_to_id IS NULL

    UNION ALL

    SELECT
      iter.issue_id,
      iter.moved_to_id,
      ARRAY_INSERT(anchor.issue_lineage, 0, iter.issue_id) AS issue_lineage
    FROM recursive_cte AS anchor
    INNER JOIN issues AS iter
      ON iter.moved_to_id = anchor.issue_id

), final AS (

    SELECT
      issue_id                                                  AS issue_id,
      issue_lineage,
      issue_lineage[ARRAY_SIZE(issue_lineage) - 1]::NUMBER      AS last_moved_issue_id,
      IFF(last_moved_issue_id != issue_id, TRUE, FALSE)         AS is_issue_moved,
      --return final common dimension mapping,
      last_moved_issue_id                                       AS dim_issue_id
    FROM recursive_cte

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-10-12",
    updated_date="2021-10-12",
) }}
