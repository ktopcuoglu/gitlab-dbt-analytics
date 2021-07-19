{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_issue', 'prep_issue')
]) }}

, gitlab_dotcom_issue_severity_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issuable_severities_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_issuable_severities_source.issue_severity_id     AS dim_issue_severity_id,
      -- FOREIGN KEYS
      gitlab_dotcom_issuable_severities_source.issue_id     AS dim_issue_id,
      --
      gitlab_dotcom_issuable_severities_source.severity     AS severity      

    FROM gitlab_dotcom_issue_severity_source
    LEFT JOIN prep_issue
      ON gitlab_dotcom_issue_severity_source.issue_id = prep_issue.dim_issue_id

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-07-15",
    updated_date="2021-07-15"
) }}
