{{ simple_cte([
    ('dates', 'dim_date'),
    ('namespace_memberships', 'gitlab_dotcom_memberships_prep'),
    ('bdg_namespace_subscription', 'bdg_namespace_order_subscription_monthly')
]) }}

, ultimate_parent_memberships AS (

    SELECT *
    FROM namespace_memberships
    WHERE namespace_id = ultimate_parent_id

), member_counts AS (

    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                   AS snapshot_month,
      namespace_id                                        AS dim_namespace_id,
      COUNT(DISTINCT user_id)
        OVER(PARTITION BY ultimate_parent_id)             AS total_member_bot_requested_count,
      COUNT(DISTINCT IFF(is_active, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS active_member_count,
      COUNT(DISTINCT IFF(is_billable, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS billable_member_count,
      COUNT(DISTINCT IFF(is_guest, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS guest_member_count,
      COUNT(DISTINCT IFF(is_deactivated, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS deactivated_member_count,
      COUNT(DISTINCT IFF(is_blocked, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS blocked_member_count,
      COUNT(DISTINCT IFF(is_project_bot, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS project_bot_count,
      COUNT(DISTINCT IFF(is_gitlab_bot, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS gitlab_bot_count,
      active_member_count
      + deactivated_member_count
      + blocked_member_count                              AS total_member_count,
      project_bot_count
      + gitlab_bot_count                                  AS total_bot_count,
      total_member_count
      + total_bot_count                                   AS total_member_bot_count,
      total_member_bot_requested_count
      - total_member_bot_count                            AS member_awaiting_access_count
    FROM ultimate_parent_memberships
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        namespace_id,
        snapshot_month
      ORDER BY namespace_id
    ) = 1

), joined AS (

    SELECT
      member_counts.snapshot_month,
      member_counts.dim_namespace_id,
      bdg_namespace_subscription.dim_subscription_id,
      member_counts.billable_member_count,
      member_counts.active_member_count,
      member_counts.guest_member_count,
      member_counts.deactivated_member_count,
      member_counts.blocked_member_count,
      member_counts.project_bot_count,
      member_counts.gitlab_bot_count,
      member_counts.total_member_count,
      member_counts.total_bot_count,
      member_counts.total_member_bot_count,
      member_counts.member_awaiting_access_count,
      member_counts.total_member_bot_requested_count
    FROM member_counts
    LEFT JOIN bdg_namespace_subscription
      ON member_counts.dim_namespace_id = bdg_namespace_subscription.dim_namespace_id
      AND member_counts.snapshot_month = bdg_namespace_subscription.snapshot_month

)

{{ dbt_audit(
cte_ref="joined",
created_by="@ischweickartDD",
updated_by="@ischweickartDD",
created_date="2021-01-07",
updated_date="2021-05-24"
) }}