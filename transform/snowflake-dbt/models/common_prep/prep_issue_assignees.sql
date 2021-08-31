{{ config(
    tags=["product"]
) }}


WITH gitlab_dotcom_issue_assignees_source AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_issue_assignees_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_issue_assignees_source.user_issue_relation_id     AS dim_user_issue_relation_id,
      gitlab_dotcom_issue_assignees_source.user_id                    AS dim_user_id,
      gitlab_dotcom_issue_assignees_source.issue_id                   AS dim_issue_id  
    FROM gitlab_dotcom_issue_assignees_source

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-09-03",
    updated_date="2021-09-03"
) }}