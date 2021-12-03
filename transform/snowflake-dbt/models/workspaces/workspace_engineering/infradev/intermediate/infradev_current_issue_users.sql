{{ config(
    materialized='ephemeral'
) }}

WITH 
  issue_assignees AS (
    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_assignees_source') }} 
  ),
  users AS (
    SELECT *
    FROM {{ ref('prep_user') }} 
  ),
   assigend_users AS (
    SELECT
      issue_id                               AS dim_issue_id,
      LISTAGG(DISTINCT users.username, ', ') AS assigned_usernames
    FROM issue_assignees
    LEFT JOIN users
      ON issue_assignees.user_id = users.dim_user_id
    GROUP BY 1
  )

  SELECT *
  FROM assigend_users