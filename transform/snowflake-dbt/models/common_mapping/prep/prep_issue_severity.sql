{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels')
]) }}

, gitlab_dotcom_issue_severity_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issuable_severities_source')}}

), severity_determination AS (
  
    SELECT
      gitlab_dotcom_issue_severity_source.issue_id     AS dim_issue_id,
      -- if issue is GitLab Incident, uses built in Severity label, otherwise try to pull from issue labels
      CASE 
        WHEN gitlab_dotcom_issue_severity_source.severity = 4 THEN '1'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%1%' THEN '1'
        WHEN gitlab_dotcom_issue_severity_source.severity = 3 THEN '2'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%2%' THEN ' 2'
        WHEN gitlab_dotcom_issue_severity_source.severity = 2 THEN '3'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%3%' THEN '3'
        WHEN gitlab_dotcom_issue_severity_source.severity = 1 THEN '4'
        WHEN LOWER(prep_labels.label_title) LIKE 'sev%4%' THEN '4'
        ELSE NULL
      END AS severity   
    FROM gitlab_dotcom_issue_severity_source
    LEFT JOIN prep_label_links
      ON gitlab_dotcom_issue_severity_source.issue_id = prep_label_links.dim_issue_id
    LEFT JOIN prep_labels 
      ON prep_label_links.dim_label_id = prep_labels.dim_label_id

), renamed AS (

    SELECT
      dim_issue_id, 
      min(severity) as severity
    FROM severity_determination
    GROUP BY 1

)





{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-07-15",
    updated_date="2021-07-15"
) }}
