WITH counts AS (

    SELECT DISTINCT
      DATE_TRUNC('month', CURRENT_DATE)                                                     AS snapshot_month,
      namespace_id,
      ultimate_parent_id,
      ultimate_parent_plan_id,
      ultimate_parent_plan_title,
      COUNT(DISTINCT IFF(is_active, user_id, NULL)) OVER(PARTITION BY ultimate_parent_id)   AS active_member_count,
      COUNT(DISTINCT IFF(is_billable, user_id, NULL)) OVER(PARTITION BY ultimate_parent_id) AS billabe_member_count,
      COUNT(DISTINCT IFF(is_guest, user_id, NULL)) OVER(PARTITION BY ultimate_parent_id)    AS guest_member_count
    FROM {{ ref('gitlab_dotcom_memberships_prep') }}
    WHERE ultimate_parent_id = namespace_id

)

{{ dbt_audit(
cte_ref="counts",
created_by="@ischweickartDD",
updated_by="@ischweickartDD",
created_date="2021-01-06",
updated_date="2021-01-06"
) }}
