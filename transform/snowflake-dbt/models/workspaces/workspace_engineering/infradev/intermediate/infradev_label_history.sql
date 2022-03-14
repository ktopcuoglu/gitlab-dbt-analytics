{{ config(
    materialized='ephemeral'
) }}

WITH labels AS (

    SELECT *
    FROM {{ ref('prep_labels') }} 

), label_links AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source') }} 

), label_type AS (

    SELECT
      dim_label_id,
      label_title,
      CASE
        WHEN LOWER(label_title) IN ('severity::1', 'severity::2', 'severity::3', 'severity::4') THEN 'severity'
        WHEN LOWER(label_title) LIKE 'team%' THEN 'team'
        WHEN LOWER(label_title) LIKE 'group%' THEN 'team'
        ELSE 'other'
      END AS label_type
    FROM labels

),  base_labels AS (

    SELECT
      label_links.label_link_id                                                                                           AS dim_issue_id,
      label_type.label_title,
      label_type.label_type,
      label_links.label_link_created_at                                                                                   AS label_added_at,
      label_links.label_link_created_at                                                                                   AS label_valid_from,
      LEAD(label_links.label_link_created_at, 1, CURRENT_DATE())
           OVER (PARTITION BY label_links.label_link_id,label_type.label_type ORDER BY label_links.label_link_created_at) AS label_valid_to
    FROM label_type
    LEFT JOIN label_links
      ON label_type.dim_label_id = label_links.target_id
      AND label_links.target_type = 'Issue'
    WHERE label_type.label_type != 'other'
      AND label_links.label_link_id IS NOT NULL

),  label_groups AS (

    SELECT
      severity.dim_issue_id,
      severity.label_title                                                      AS severity_label,
      team.label_title                                                          AS team_label,
      'S' || RIGHT(severity_label, 1)                                           AS severity,
      SPLIT(team_label, '::')[ARRAY_SIZE(SPLIT(team_label, '::')) - 1]::VARCHAR AS assigned_team,
      severity.label_added_at                                                   AS severity_label_added_at,
      team.label_added_at                                                       AS team_label_added_at,
      GREATEST(severity.label_valid_from, team.label_valid_from)                AS label_group_valid_from,
      LEAST(severity.label_valid_to, team.label_valid_to)                       AS label_group_valid_to
    FROM base_labels AS severity
    INNER JOIN base_labels AS team
      ON severity.dim_issue_id = team.dim_issue_id
    WHERE severity.label_type = 'severity'
      AND team.label_type = 'team'
      
)

SELECT *
FROM label_groups