WITH issues AS (
  
    SELECT *
    FROM {{ ref ('gitlab_dotcom_issues_xf') }}
    WHERE project_id =16492321 --Recruiting for Open Positions

), users AS (

    SELECT *
    FROM {{ ref ('gitlab_dotcom_users') }}

), assignee AS (

    SELECT 
      assignee.*, 
      user_name AS assignee
    FROM {{ ref ('gitlab_dotcom_issue_assignees') }} assignee
    LEFT JOIN users
      ON assignee.user_id = users.user_id 

), agg_assignee AS (

    SELECT
     issue_id,
     ARRAY_AGG(LOWER(assignee)) WITHIN GROUP (ORDER BY assignee ASC) AS assignee
    FROM assignee
    GROUP BY issue_id

), intermediate AS (

    SELECT 
      issues.issue_title,
      issues.issue_iid,
      issues.issue_created_at,
      DATE_TRUNC(week,issue_created_at)                         AS issue_created_week,
      issues.issue_closed_at,
      DATE_TRUNC(week,issues.issue_closed_at)                   AS issue_closed_week,
      IFF(issue_closed_at IS NOT NULL,1,0)                      AS is_issue_closed,
      issues.state                                              AS issue_state,
      agg_assignee.assignee,
      IFF(CONTAINS(issue_description, '[x] Yes, Diversity Sourcing methods were used'::VARCHAR) = True,
        'Used Diversity Strings', NULL)                         AS is_using_diversity_strings,
      IFF(CONTAINS(issue_description, '[x] No, I did not use Diversity Sourcing methods'::VARCHAR) = True,
        'Did not Use', NULL)                                    AS is_not_using_diversity_srings,
      IFF(used_diversity_booleanstrings IS NULL AND did_not_use IS NULL, 
        'No Answer',NULL)                                       AS has_no_Answer
    FROM issues
    LEFT JOIN agg_assignee 
      ON agg_assignee.issue_id = issues.issue_id
    WHERE project_id = 16492321
      AND LOWER(issue_title) LIKE '%weekly check-in:%'
      AND LOWER(issue_title) NOT LIKE '%test%'
  
)

SELECT *
FROM intermediate
