{{config({
   "schema": "restricted_safe_common_mart_product"
 })}}

{{ simple_cte([
    ('bdg_epic_user_request', 'bdg_epic_user_request'),
    ('bdg_issue_user_request', 'bdg_issue_user_request'),
    ('dim_epic', 'dim_epic'),
    ('dim_issue', 'dim_issue'),
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels'),
    ('fct_mrr', 'fct_mrr'),
    ('dim_date', 'dim_date'),
    ('dim_product_detail', 'dim_product_detail')
])}}

, arr_metrics_current_month AS (

    SELECT
      fct_mrr.dim_crm_account_id,
      SUM(fct_mrr.mrr)                                                               AS mrr,
      SUM(fct_mrr.arr)                                                               AS arr,
      SUM(fct_mrr.quantity)                                                          AS quantity
    FROM fct_mrr
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND dim_date.date_actual = DATE_TRUNC('month', CURRENT_DATE)
      AND dim_product_detail.product_tier_name IN ('Plus', 'GitHost', 'Standard', 'Self-Managed - Starter', 'Self-Managed - Premium',
        'SaaS - Premium', 'SaaS - Bronze', 'Basic', 'Self-Managed - Ultimate', 'SaaS - Ultimate')
    GROUP BY 1

), epic_weight AS (

    SELECT
      dim_epic_id,
      SUM(weight)                                                             AS epic_weight,
      SUM(IFF(state_name = 'closed', weight, 0)) / NULLIFZERO(epic_weight)    AS epic_completeness,
      SUM(IFF(state_name = 'closed', 1, 0)) / COUNT(*)                        AS epic_completeness_alternative,
      COALESCE(epic_completeness, epic_completeness_alternative)              AS epic_status
    FROM dim_issue
    GROUP BY 1
    
), label_links_joined AS (

    SELECT
      prep_label_links.*,
      prep_labels.label_title
    FROM prep_label_links
    LEFT JOIN prep_labels
      ON prep_label_links.dim_label_id = prep_labels.dim_label_id

), issue_group_label AS (

    SELECT
      label_links_joined.dim_issue_id,
      IFF(LOWER(label_links_joined.label_title) LIKE 'group::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS group_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'devops::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)  AS devops_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'section::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL) AS section_label,
      COALESCE(group_label, devops_label, section_label)                                                        AS product_group,
      CASE
        WHEN group_label IS NOT NULL THEN 3
        WHEN devops_label IS NOT NULL THEN 2
        WHEN section_label IS NOT NULL THEN 1
        ELSE 0
      END product_group_level
    FROM label_links_joined
    WHERE product_group IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY label_links_joined.dim_issue_id ORDER BY product_group_level DESC) = 1

), issue_status AS (

    SELECT
      label_links_joined.dim_issue_id,
      IFF(LOWER(label_links_joined.label_title) LIKE 'workflow::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS workflow_label
    FROM label_links_joined

), epic_group_label AS (

    SELECT
      label_links_joined.dim_epic_id,
      IFF(LOWER(label_links_joined.label_title) LIKE 'group::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)   AS group_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'devops::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL)  AS devops_label,
      IFF(LOWER(label_links_joined.label_title) LIKE 'section::%', SPLIT_PART(label_links_joined.label_title, '::', 2), NULL) AS section_label,
      COALESCE(group_label, devops_label, section_label)                                                    AS product_group,
      CASE
        WHEN group_label IS NOT NULL THEN 3
        WHEN devops_label IS NOT NULL THEN 2
        WHEN section_label IS NOT NULL THEN 1
        ELSE 0
      END product_group_level
    FROM label_links_joined
    WHERE product_group IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY label_links_joined.dim_epic_id ORDER BY product_group_level DESC) = 1

), epic_last_milestone AS ( -- Get issue milestone with the latest due dates for epics
    
    SELECT
      dim_epic_id,
      milestone_title,
      milestone_due_date
    FROM dim_issue
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_epic_id ORDER BY milestone_due_date DESC NULLS LAST) = 1

), user_request AS (

    SELECT
      bdg_issue_user_request.dim_issue_id                                       AS dim_issue_id,
      dim_issue.dim_epic_id                                                     AS dim_epic_id,
      'Issue'                                                                   AS user_request_in,

      bdg_issue_user_request.link_type                                          AS link_type,
      bdg_issue_user_request.dim_crm_opportunity_id                             AS dim_crm_opportunity_id,
      bdg_issue_user_request.dim_crm_account_id                                 AS dim_crm_account_id,
      bdg_issue_user_request.dim_ticket_id                                      AS dim_ticket_id,
      bdg_issue_user_request.request_priority                                   AS request_priority,
      bdg_issue_user_request.is_user_request_only_in_collaboration_project      AS is_user_request_only_in_collaboration_project,

      -- Epic / Issue attributes
      dim_issue.issue_title                                                     AS issue_epic_title,
      dim_issue.issue_url                                                       AS issue_epic_url,
      dim_issue.milestone_title                                                 AS milestone_title,
      dim_issue.milestone_due_date                                              AS milestone_due_date,
      dim_issue.labels                                                          AS issue_epic_labels,
      IFNULL(ARRAY_CONTAINS('deliverable'::VARIANT, dim_issue.labels), FALSE)   AS has_deliverable_label,
      IFNULL(issue_group_label.product_group, 'Unknown')                        AS product_group,
      issue_status.workflow_label                                               AS issue_status,
      NULL                                                                      AS epic_status,
      dim_epic.epic_url                                                         AS parent_epic_path,
      dim_epic.epic_title                                                       AS parent_epic_title,
      dim_issue.upvote_count                                                    AS upvote_count,
      dim_issue.weight                                                          AS issue_epic_weight,

      -- CRM Account attributes
      arr_metrics_current_month.quantity                                        AS customer_reach,
      arr_metrics_current_month.arr                                             AS customer_arr

    FROM bdg_issue_user_request
    LEFT JOIN dim_issue
      ON dim_issue.dim_issue_id = bdg_issue_user_request.dim_issue_id
    LEFT JOIN issue_group_label
      ON issue_group_label.dim_issue_id = bdg_issue_user_request.dim_issue_id
    LEFT JOIN issue_status
      ON issue_status.dim_issue_id = bdg_issue_user_request.dim_issue_id
    LEFT JOIN dim_epic
      ON dim_epic.dim_epic_id = dim_issue.dim_epic_id
    LEFT JOIN arr_metrics_current_month
      ON arr_metrics_current_month.dim_crm_account_id = bdg_issue_user_request.dim_crm_account_id

    UNION

    SELECT
      NULL                                                                        AS dim_issue_id,
      bdg_epic_user_request.dim_epic_id                                           AS dim_epic_id,
      'Epic'                                                                      AS user_request_in,
      
      bdg_epic_user_request.link_type                                             AS link_type,
      bdg_epic_user_request.dim_crm_opportunity_id                                AS dim_crm_opportunity_id,
      bdg_epic_user_request.dim_crm_account_id                                    AS dim_crm_account_id,
      bdg_epic_user_request.dim_ticket_id                                         AS dim_ticket_id,
      bdg_epic_user_request.request_priority                                      AS request_priority,
      bdg_epic_user_request.is_user_request_only_in_collaboration_project         AS is_user_request_only_in_collaboration_project,

      -- Epic / Issue attributes
      dim_epic.epic_title                                                         AS epic_title,
      dim_epic.epic_url                                                           AS epic_url,
      epic_last_milestone.milestone_title                                         AS milestone_title,
      epic_last_milestone.milestone_due_date                                      AS milestone_due_date,
      dim_epic.labels                                                             AS issue_epic_labels,
      IFNULL(ARRAY_CONTAINS('deliverable'::VARIANT, dim_epic.labels), 'FALSE')    AS has_deliverable_label,
      IFNULL(epic_group_label.product_group, 'Unknown')                           AS product_group,
      'Not Applicable'                                                            AS issue_status,
      epic_weight.epic_status                                                     AS epic_status,
      parent_epic.epic_url                                                        AS parent_epic_path,
      parent_epic.epic_title                                                      AS parent_epic_title,
      dim_epic.upvote_count                                                       AS upvote_count,
      epic_weight.epic_weight                                                     AS issue_epic_weight,

      -- CRM Account attributes
      arr_metrics_current_month.quantity                                          AS customer_reach,
      arr_metrics_current_month.arr                                               AS customer_arr

    FROM bdg_epic_user_request
    LEFT JOIN dim_issue
      ON dim_issue.dim_epic_id = bdg_epic_user_request.dim_epic_id
    LEFT JOIN dim_epic
      ON dim_epic.dim_epic_id = bdg_epic_user_request.dim_epic_id
    LEFT JOIN epic_last_milestone
      ON epic_last_milestone.dim_epic_id = bdg_epic_user_request.dim_epic_id
    LEFT JOIN epic_group_label
      ON epic_group_label.dim_epic_id = bdg_epic_user_request.dim_epic_id
    LEFT JOIN epic_weight
      ON epic_weight.dim_epic_id = bdg_epic_user_request.dim_epic_id
    LEFT JOIN dim_epic AS parent_epic
      ON parent_epic.dim_epic_id = dim_epic.parent_id
    LEFT JOIN arr_metrics_current_month
      ON arr_metrics_current_month.dim_crm_account_id = bdg_epic_user_request.dim_crm_account_id

)


{{ dbt_audit(
    cte_ref="user_request",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-10-22",
    updated_date="2021-10-22",
  ) }}
