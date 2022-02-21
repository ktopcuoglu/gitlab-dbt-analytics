{{ config(
    tags=["product"]
) }}

With gitlab_dotcom_issue_links_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_links_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_issue_links_source.issue_link_id  AS dim_issue_link_id,
      -- FOREIGN KEYS
      gitlab_dotcom_issue_links_source.source_id      AS dim_source_issue_id,
      --
      gitlab_dotcom_issue_links_source.target_id      AS dim_target_issue_id,
      gitlab_dotcom_issue_links_source.created_at,
      gitlab_dotcom_issue_links_source.updated_at,
      valid_from
    FROM gitlab_dotcom_issue_links_source


)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-08-04",
    updated_date="2021-08-04"
) }}
