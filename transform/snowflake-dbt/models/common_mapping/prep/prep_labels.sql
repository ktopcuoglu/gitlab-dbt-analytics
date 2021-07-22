{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_project', 'prep_project')
]) }}

, gitlab_dotcom_labels_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_labels_source')}}

), renamed AS (
  
    SELECT
      gitlab_dotcom_labels_source.label_id       AS dim_label_id,
      -- FOREIGN KEYS
      gitlab_dotcom_labels_source.project_id     AS dim_project_id,
      --
      gitlab_dotcom_labels_source.group_id       AS dim_namespace_id,
      gitlab_dotcom_labels_source.label_title,
      gitlab_dotcom_labels_source.label_type
    FROM gitlab_dotcom_labels_source


)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-07-15",
    updated_date="2021-07-15"
) }}