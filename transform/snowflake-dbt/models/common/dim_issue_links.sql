WITH prep_issue_links AS (

    SELECT 
      dim_issue_link_id,
      -- FOREIGN KEYS
      -- joins to dim_issue_id in dim_issue table
      dim_source_issue_id,
      dim_target_issue_id,
      --
      created_at,
      updated_at,
      valid_from
    FROM {{ ref('prep_issue_links') }}

)

{{ dbt_audit(
    cte_ref="prep_issue_links",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-08-04",
    updated_date="2021-08-04"
) }}