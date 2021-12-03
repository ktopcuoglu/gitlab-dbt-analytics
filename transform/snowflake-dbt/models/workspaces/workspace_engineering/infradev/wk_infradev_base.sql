with issues AS (
  SELECT
    *
  FROM {{ ref('base_infradev_issues') }} 

),

dates AS (
  SELECT
    *,
    MIN(date_id) OVER () AS min_date_id
  FROM {{ ref('dim_date') }}  
  WHERE date_actual > DATE_TRUNC('month', DATEADD('year', -2, CURRENT_DATE()))
    AND date_actual < CURRENT_DATE()
),

projects AS (
  SELECT * 
  FROM {{ ref('dim_project') }}
),

assigend_users as (
  SELECT
    *
  FROM {{ ref('infradev_current_issue_users') }} 
  
),

label_groups as (
  SELECT
    *
  FROM {{ ref('infradev_label_history') }} 
  
),

namespace_path as (
  SELECT
    *
  FROM {{ ref('infradev_namespace_path') }}
)

SELECT --count(*)
  dates.date_actual,
  issues.dim_issue_id,
  issues.issue_internal_id,
  issues.dim_project_id,
  issues.dim_namespace_id,
  issues.labels,
  issues.issue_title,
  namespace_path.full_namespace_path,
    '[' || REPLACE(REPLACE(LEFT(issues.issue_title, 64), '[', ''), ']', '') || '](https://gitlab.com/' ||
    namespace_path.full_namespace_path || '/' || projects.project_path || '/issues/' || issues.issue_internal_id ||
    ')'                                                                    AS issue_url,
  IFF(dates.date_actual > issues.issue_closed_at, 'closed', 'open')        AS issue_state,
  issues.created_at                                                        AS issue_created_at,
  issues.issue_closed_at,
  IFNULL(label_groups.severity, 'No Severity')                             AS severity,
  label_groups.severity_label_added_at,
  IFNULL(label_groups.assigned_team, 'Unassigned')                         AS assigned_team,
  label_groups.team_label_added_at,
  label_groups.team_label,
  IFF(dates.date_actual > issues.issue_closed_at, NULL,
      DATEDIFF('day', issues.created_at, dates.date_actual))               AS issue_open_age_in_days,
  DATEDIFF('day', label_groups.severity_label_added_at, dates.date_actual) AS severity_label_age_in_days,
  assigend_users.assigned_usernames,
  IFF(assigend_users.assigned_usernames IS NULL, TRUE, FALSE)              AS is_issue_unassigned
FROM issues -- 521,491
INNER JOIN dates -- 108,777,470
  ON issues.created_date_id <= dates.date_id
  --and issues.created_date_id > dates.min_date_id -- inlcude all open issues
LEFT JOIN projects --
  ON issues.dim_project_id = projects.dim_project_id
LEFT JOIN namespace_path --
  ON issues.dim_namespace_id = namespace_path.dim_namespace_id
LEFT JOIN assigend_users
  ON issues.dim_issue_id = assigend_users.dim_issue_id
  --left join issue_assignees -- 675270
  --on issues.dim_issue_id = issue_assignees.issue_id
--left join users -- 675270
  --on issue_assignees.user_id = users.dim_user_id
LEFT JOIN label_groups -- 108,780,372
  ON issues.dim_issue_id = label_groups.dim_issue_id
  AND
     dates.date_actual BETWEEN DATE_TRUNC('day', label_groups.label_group_valid_from) AND DATE_TRUNC('day', label_groups.label_group_valid_to)
--inner join dates -- 44,174,012  10,126,698
--on dates.date_actual BETWEEN DATE_TRUNC('day',lable_groups.label_group_valid_from) AND DATE_TRUNC('day',lable_groups.label_group_valid_to)
--where issue_state = 'open'