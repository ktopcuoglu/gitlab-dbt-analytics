{{ config(
    tags=["product"]
) }}


{{ simple_cte([
    ('prep_issue_labels', 'prep_issue_labels'),
    ('dim_date', 'dim_date'),
    ('prep_issue', 'prep_issue')
]) }}

, severity_labels_over_time AS (
    SELECT *
    FROM prep_issue_labels
    WHERE label_name IN ('severity::1','severity::2','severity::3','severity::4') 


) , joined AS (

    SELECT
      prep_issue_labels.date_id,
      prep_issue_labels.dim_issue_id,
      prep_issue_labels.date_actual,
      -- calculate age since issue was opened
      CASE
        -- date at point in time is after issue close date, don't show this in results
        WHEN prep_issue_labels.date_actual > prep_issue.issue_closed_at THEN NULL 
        -- issue still open, calc age from issue creation to point in time
        WHEN prep_issue.issue_closed_at IS NULL THEN DATEDIFF('DAY',prep_issue.created_at,prep_issue_labels.date_actual)
        -- issue is closed, but date at point in time is before issue close date age is from creation date to point in time
        ELSE DATEDIFF('DAY',prep_issue.created_at,prep_issue_labels.date_actual)
        END AS open_age_in_days, 
        -- calculate age since severity label was added 
      CASE
        -- date at point in time is after issue close date, don't show this in results
        WHEN prep_issue_labels.date_actual > prep_issue.issue_closed_at THEN NULL 
        -- issue still open, calc age from issue creation to point in time
        WHEN prep_issue.issue_closed_at IS NULL THEN DATEDIFF('DAY',sev_labels.label_added_at,prep_issue_labels.date_actual)
        -- issue is closed, but date at point in time is before issue close date age is from creation date to point in time
        ELSE DATEDIFF('DAY',sev_labels.label_added_at,prep_issue_labels.date_actual)
      END AS open_age_after_sev_label_added,
      sev_labels.label_name AS severity,
      prep_issue.labels
    FROM prep_issue
    LEFT JOIN prep_issue_labels
      ON prep_issue_labels.dim_issue_id = prep_issue.dim_issue_id
    LEFT JOIN dim_date
      ON prep_issue_labels.date_id = dim_date.date_id
    LEFT JOIN severity_labels_over_time sev_labels
      ON sev_labels.dim_issue_id = prep_issue.dim_issue_id
      AND sev_labels.date_actual = dim_date.date_actual

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@dtownsend",
    updated_by="@dtownsend",
    created_date="2021-10-01",
    updated_date="2021-10-01"
) }}