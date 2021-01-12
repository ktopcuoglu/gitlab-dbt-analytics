WITH counts AS (

    SELECT DISTINCT
      DATE_TRUNC('month', CURRENT_DATE)                   AS snapshot_month,
      namespace_id,
      ultimate_parent_id,
      ultimate_parent_plan_id,
      ultimate_parent_plan_title,
      COUNT(DISTINCT user_id)
        OVER(PARTITION BY ultimate_parent_id)             AS total_member_bot_requested_count,
      COUNT(DISTINCT IFF(is_active, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS active_member_count,
      COUNT(DISTINCT IFF(is_billable, user_id, NULL))
        OVER(PARTITION BY ultimate_parent_id)             AS billabe_member_count,
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
    FROM {{ ref('gitlab_dotcom_memberships_prep') }}
    WHERE ultimate_parent_id = namespace_id

)

{{ dbt_audit(
cte_ref="counts",
created_by="@ischweickartDD",
updated_by="@ischweickartDD",
created_date="2021-01-07",
updated_date="2021-01-07"
) }}
