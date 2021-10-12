WITH gitlab_dotcom_projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_source') }}
  
), issue_links AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_links_source') }}
    WHERE is_currently_valid

), issue_notes AS (
  
    SELECT
      noteable_id AS issue_id,
      *
    FROM {{ ref('gitlab_dotcom_notes_source') }}
    WHERE noteable_type = 'Issue'
      AND system = FALSE
  
), gitlab_issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY project_id, issue_iid ORDER BY created_at DESC) = 1 

), collaboration_projects AS (

    SELECT
      account_id,
      gitlab_customer_success_project
    FROM {{ ref('sfdc_account_source') }}
    WHERE gitlab_customer_success_project LIKE '%gitlab.com/%'

), gitlab_dotcom_project_routes AS (

    SELECT
      'https://gitlab.com/' || path AS complete_path,
      source_id                     AS project_id,
      *
    FROM {{ ref('gitlab_dotcom_routes_source') }}
    WHERE source_type = 'Project'

), collaboration_projects_with_ids AS (

    SELECT
      collaboration_projects.*,
      gitlab_dotcom_project_routes.project_id AS collaboration_project_id,
      gitlab_issues.issue_id,
      gitlab_issues.issue_description
    FROM collaboration_projects
    LEFT JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.complete_path = collaboration_projects.gitlab_customer_success_project
    LEFT JOIN gitlab_issues
      ON gitlab_issues.project_id = gitlab_dotcom_project_routes.project_id

), collaboration_projects_issue_descriptions AS (

    SELECT
      *,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_description, '(?<=gitlab.com\/)gitlab-org\/.*issues\/[0-9]{1,10}')      AS issue_links
    FROM collaboration_projects_with_ids
    WHERE ARRAY_SIZE(issue_links) != 0

), collaboration_projects_issue_descriptions_parsed AS (

    SELECT
      collaboration_projects_issue_descriptions.*,
      f.value AS user_request_issue_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss') AS user_request_issue_path_fixed,
      SPLIT_PART(f.value, '/', -1) AS user_request_issue_iid,
      RTRIM(SPLIT_PART(f.value, '/issues', 1), '/-') AS user_request_project_path
    FROM collaboration_projects_issue_descriptions,
      TABLE(FLATTEN(issue_links)) f

), collaboration_projects_issue_notes AS (

    SELECT 
      collaboration_projects_with_ids.*,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_notes.note, '(?<=gitlab.com\/)gitlab-org\/.*issues\/[0-9]{1,10}') AS issue_links
    FROM collaboration_projects_with_ids
    LEFT JOIN issue_notes
      ON issue_notes.issue_id = collaboration_projects_with_ids.issue_id
    WHERE ARRAY_SIZE(issue_links) != 0

), collaboration_projects_issue_notes_parsed AS (

    SELECT
      collaboration_projects_issue_notes.*,
      f.value AS user_request_issue_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss')  AS user_request_issue_path_fixed,
      SPLIT_PART(f.value, '/', -1)                                                  AS user_request_issue_iid,
      RTRIM(SPLIT_PART(f.value, '/issues', 1), '/-')                                AS user_request_project_path
    FROM collaboration_projects_issue_notes,
      TABLE(FLATTEN(issue_links)) f

), collaboration_projects_issue_description_notes_unioned AS (

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_issue_iid,
      user_request_project_path
    FROM collaboration_projects_issue_notes_parsed

    UNION

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_issue_iid,
      user_request_project_path
    FROM collaboration_projects_issue_descriptions_parsed

), unioned_with_user_request_project_id AS (

    SELECT
      collaboration_projects_issue_description_notes_unioned.*,
      gitlab_dotcom_project_routes.project_id AS user_request_project_id
    FROM collaboration_projects_issue_description_notes_unioned
    INNER JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.path = collaboration_projects_issue_description_notes_unioned.user_request_project_path
    INNER JOIN gitlab_dotcom_projects
      ON gitlab_dotcom_projects.project_id = gitlab_dotcom_project_routes.project_id

), unioned_with_issue_links AS (

    SELECT
      unioned_with_user_request_project_id.account_id                       AS dim_crm_account_id,
      unioned_with_user_request_project_id.collaboration_project_id         AS dim_collaboration_project_id,
      gitlab_issues.issue_id                                                AS dim_user_request_issue_id,
      unioned_with_user_request_project_id.user_request_project_id          AS dim_user_request_project_id,
      unioned_with_user_request_project_id.gitlab_customer_success_project,
      unioned_with_user_request_project_id.user_request_issue_iid
    FROM unioned_with_user_request_project_id
    INNER JOIN gitlab_issues
      ON gitlab_issues.project_id = unioned_with_user_request_project_id.user_request_project_id
      AND gitlab_issues.issue_iid = unioned_with_user_request_project_id.user_request_issue_iid

    UNION

    SELECT
      collaboration_projects_with_ids.account_id                            AS dim_crm_account_id,
      collaboration_projects_with_ids.collaboration_project_id              AS dim_collaboration_project_id,
      gitlab_issues.issue_id                                                AS dim_user_request_issue_id,
      gitlab_issues.project_id                                              AS dim_user_request_project_id,
      collaboration_projects_with_ids.gitlab_customer_success_project,
      gitlab_issues.issue_iid                                               AS user_request_issue_internal_id
    FROM collaboration_projects_with_ids
    INNER JOIN issue_links
      ON issue_links.source_id = collaboration_projects_with_ids.issue_id
    INNER JOIN gitlab_issues
      ON gitlab_issues.issue_id = issue_links.target_id
    INNER JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.project_id = gitlab_issues.project_id
    WHERE gitlab_dotcom_project_routes.path LIKE 'gitlab-org%'

)

SELECT *
FROM unioned_with_issue_links
