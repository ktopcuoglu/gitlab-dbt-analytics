{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_issue', 'dim_issue'),
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels')
]) }}

, final AS (
    SELECT
    -- FOREIGN KEYS
    dim_date.date_id,
    dim_date.date_actual,
    dim_issue.dim_issue_id                          AS dim_issue_id,
    prep_labels.dim_label_id                        AS dim_label_id,
    LOWER(prep_labels.label_name)                   AS label_name,
    prep_label_links.label_added_at                 AS label_added_at,
    prep_label_links.is_currently_valid 

    FROM dim_issue
    INNER JOIN prep_label_links
        ON dim_issue.issue_id = prep_label_links.target_id
    INNER JOIN prep_labels
        ON prep_label_links.label_id = prep_labels.label_id
    LEFT JOIN dim_date 
        ON dim_date.date_actual BETWEEN prep_label_links.label_added_at AND COALESCE(prep_label_links.valid_to,CURRENT_DATE)
)

 {{ dbt_audit(
    cte_ref="final",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-09-03",
    updated_date="2021-09-03"
) }}