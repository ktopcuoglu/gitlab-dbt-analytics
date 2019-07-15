{{ config({
    "schema": "analytics",
    "materialized": "incremental",
    "unique_key": "group_month_unique_id",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH months AS (

    SELECT DISTINCT
      first_day_of_month AS skeleton_month
    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < CURRENT_DATE
      {% if is_incremental() %}
        AND first_day_of_month >= (SELECT MAX(audit_event_month) from {{ this }})
      {% endif %}

), groups AS (

    SELECT
      namespaces.namespace_id                            AS group_id,
      namespaces.parent_id                               AS parent_group_id,
      IFF(namespaces.parent_id IS NULL, True, False)     AS is_top_level_group,
      DATE_TRUNC(month, namespaces.namespace_created_at) AS group_created_at_month
    FROM {{ ref('gitlab_dotcom_namespaces') }}           AS namespaces
    WHERE namespace_type = 'Group'

), skeleton AS ( -- create a framework of one row per group per month (after their creation date)

    SELECT
      groups.group_id,
      groups.parent_group_id,
      groups.is_top_level_group,
      groups.group_created_at_month,
      months.skeleton_month,
      DATEDIFF(month, groups.group_created_at_month, months.skeleton_month) AS months_since_creation_date
    FROM groups
    LEFT JOIN months
      ON DATE_TRUNC('month', group_created_at_month) <= months.skeleton_month

), audit_events AS (

    SELECT
      entity_id AS group_id,
      DATE_TRUNC(month, audit_event_created_at)  AS audit_event_month,
      COUNT(*)                                   AS audit_events_count
    FROM {{ ref('gitlab_dotcom_audit_events') }}
    WHERE entity_type = 'Group'
      {% if is_incremental() %}
        AND audit_event_created_at >= (SELECT MAX(audit_event_month) from {{ this }})
      {% endif %}
    GROUP BY 1,2

), joined AS (

    SELECT
      skeleton.group_id,
      skeleton.parent_group_id,
      skeleton.is_top_level_group,
      skeleton.group_created_at_month,
      skeleton.skeleton_month                                    AS audit_event_month,
      skeleton.months_since_creation_date,
      COALESCE(audit_events.audit_events_count, 0)               AS audit_events_count,
      IFF(audit_events_count > 0, TRUE, FALSE)                   AS group_was_active_in_month,
      {{ dbt_utils.surrogate_key('skeleton.group_id', 'skeleton_month') }} AS group_month_unique_id
    FROM skeleton
    LEFT JOIN audit_events
      ON skeleton.group_id = audit_events.group_id
        AND skeleton.skeleton_month = audit_events.audit_event_month
    ORDER BY 4 DESC, 1 DESC

)

SELECT *
FROM joined
