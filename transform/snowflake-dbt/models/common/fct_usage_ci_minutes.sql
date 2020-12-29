WITH project_snapshot_monthly_all AS (

    --project_snapshot_monthly 
    SELECT
      snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM {{ ref('gitlab_dotcom_project_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

    UNION ALL
      
    --project_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                 AS snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM {{ ref('gitlab_dotcom_projects_source') }}
    
), namespace_lineage_monthly_all AS (

    --namespace_lineage_monthly
    SELECT
      snapshot_month,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

    UNION ALL
    
    --namespace_lineage_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                 AS snapshot_month,
      namespace_id,
      parent_id,
      upstream_lineage,
      ultimate_parent_id
    FROM {{ ref('gitlab_dotcom_namespace_lineage_prep') }}

), namespace_snapshots_monthly_all AS (

    --namespace_snapshots_monthly
    SELECT
      snapshot_month,
      namespace_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM {{ ref('gitlab_dotcom_namespace_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

    UNION ALL
    
    --namespace_current
    SELECT 
      DATE_TRUNC('month', CURRENT_DATE)                 AS snapshot_month,
      namespace_id,
      parent_id,
      owner_id,
      namespace_type,
      visibility_level,
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit
    FROM {{ ref('gitlab_dotcom_namespaces_source') }} 

), namespace_statistic_monthly_all AS (

    --namespace_statistic_monthly
    SELECT 
      snapshot_month,
      namespace_id,
      shared_runners_seconds,
      shared_runners_seconds_last_reset
    FROM {{ ref('gitlab_dotcom_namespace_statistics_historical_monthly') }}
    WHERE snapshot_month >= '2020-07-01'

    UNION ALL

    --namespace_statistic_current
    SELECT
      DATE_TRUNC('month', CURRENT_DATE)                 AS snapshot_month,
      namespace_id,
      shared_runners_seconds,
      shared_runners_seconds_last_reset
    FROM {{ ref('gitlab_dotcom_namespace_statistics_source') }}
      
), child_projects_enabled_shared_runners_any AS (

    SELECT
      project_snapshot_monthly_all.snapshot_month,
      namespace_lineage_monthly_all.ultimate_parent_id,
      MAX(project_snapshot_monthly_all.shared_runners_enabled)
                                                        AS shared_runners_enabled
    FROM project_snapshot_monthly_all
    INNER JOIN namespace_lineage_monthly_all
      ON project_snapshot_monthly_all.namespace_id = namespace_lineage_monthly_all.namespace_id
      AND project_snapshot_monthly_all.snapshot_month = namespace_lineage_monthly_all.snapshot_month
    GROUP BY 1, 2

), namespace_statistic_monthly_top_level AS (

    SELECT
      namespace_statistic_monthly_all.snapshot_month    AS namespace_statistic_snapshot_month,
      namespace_statistic_monthly_all.namespace_id      AS namespace_statistic_namespace_id,
      namespace_statistic_monthly_all.shared_runners_seconds,
      namespace_statistic_monthly_all.shared_runners_seconds_last_reset,
      namespace_snapshots_monthly_all.snapshot_month    AS namespace_snapshots_snapshot_month,
      namespace_snapshots_monthly_all.namespace_id      AS namespace_snapshots_namespace_id,
      namespace_snapshots_monthly_all.parent_id,
      namespace_snapshots_monthly_all.owner_id,
      namespace_snapshots_monthly_all.namespace_type,
      namespace_snapshots_monthly_all.visibility_level,
      namespace_snapshots_monthly_all.shared_runners_minutes_limit,
      namespace_snapshots_monthly_all.extra_shared_runners_minutes_limit
    FROM namespace_statistic_monthly_all
    INNER JOIN namespace_snapshots_monthly_all
      ON namespace_statistic_monthly_all.namespace_id = namespace_snapshots_monthly_all.namespace_id
      AND namespace_statistic_monthly_all.snapshot_month = namespace_snapshots_monthly_all.snapshot_month
      AND namespace_snapshots_monthly_all.parent_id IS NULL  -- Only top level namespaces
      
), ci_minutes_logic AS (
    
    SELECT
      namespace_statistic_monthly_top_level.namespace_statistic_snapshot_month
                                                        AS snapshot_month,
      namespace_statistic_monthly_top_level.namespace_statistic_namespace_id
                                                        AS namespace_id,
      IFNULL(child_projects_enabled_shared_runners_any.ultimate_parent_id, namespace_id)
                                                        AS ultimate_parent_namespace_id,
      namespace_statistic_monthly_top_level.namespace_type,
      namespace_statistic_monthly_top_level.visibility_level,
      IFNULL(child_projects_enabled_shared_runners_any.shared_runners_enabled, FALSE)
                                                        AS shared_runners_enabled,
      namespace_statistic_monthly_top_level.shared_runners_minutes_limit,
      namespace_statistic_monthly_top_level.extra_shared_runners_minutes_limit,
      400                                               AS gitlab_current_settings_shared_runners_minutes,
      IFNULL(shared_runners_minutes_limit, gitlab_current_settings_shared_runners_minutes)
                                                        AS monthly_minutes, 
      IFNULL(extra_shared_runners_minutes_limit, 0)     AS purchased_minutes,
      IFF(purchased_minutes > 0,
          True, False)                                  AS any_minutes_purchased,
      namespace_statistic_monthly_top_level.shared_runners_seconds / 60 
                                                        AS total_minutes_used,
      IFF(
          IFF(shared_runners_minutes_limit IS NOT NULL,
              shared_runners_minutes_limit + purchased_minutes,
              gitlab_current_settings_shared_runners_minutes + purchased_minutes
              ) > 0,
          True, False)                                  AS actual_shared_runners_minutes_limit_non_zero,     
      IFF(shared_runners_enabled
            AND actual_shared_runners_minutes_limit_non_zero,
          True, False)                                  AS shared_runners_minutes_limit_enabled,
      CASE 
        WHEN actual_shared_runners_minutes_limit_non_zero
          THEN monthly_minutes::VARCHAR
        WHEN shared_runners_enabled
          THEN 'Unlimited'
        ELSE 'Not supported'
      END                                               AS limit,
      IFF(total_minutes_used <= monthly_minutes,
          True, False)                                  AS monthly_minutes_available,
      IFF(NOT any_minutes_purchased
            OR monthly_minutes_available,
          0, total_minutes_used - monthly_minutes)      AS purchased_minutes_used,
      total_minutes_used - purchased_minutes_used       AS monthly_minutes_used,
      IFF(shared_runners_minutes_limit_enabled
            AND monthly_minutes_used >= monthly_minutes,
          True, False)                                  AS monthly_minutes_used_up,
      IFF(shared_runners_minutes_limit_enabled
            AND any_minutes_purchased
            AND purchased_minutes_used >= purchased_minutes,
          True, False)                                  AS purchased_minutes_used_up,
      CASE
        WHEN NOT monthly_minutes_used_up
          THEN 'Under Quota'
        WHEN shared_runners_minutes_limit_enabled
          THEN 'Over Quota'
        ELSE 'Disabled'
      END                                               AS status,
      CASE purchased_minutes_used_up
        WHEN FALSE 
         THEN 'Under Quota'
        ELSE 'Over Quota'
      END                                               AS status_purchased,
      CASE 
        WHEN monthly_minutes_used < monthly_minutes OR NOT actual_shared_runners_minutes_limit_non_zero
          THEN 'Under Quota'
        ELSE 'Over Quota'
       END                                              AS status_based_plan
    FROM namespace_statistic_monthly_top_level
    LEFT JOIN child_projects_enabled_shared_runners_any
      ON namespace_statistic_monthly_top_level.namespace_statistic_namespace_id = child_projects_enabled_shared_runners_any.ultimate_parent_id
      AND namespace_statistic_monthly_top_level.namespace_statistic_snapshot_month = child_projects_enabled_shared_runners_any.snapshot_month

), final AS (

    SELECT
      snapshot_month,
      namespace_id,
      ultimate_parent_namespace_id,
      namespace_type,
      visibility_level, 
      total_minutes_used                                    AS shared_runners_minutes_used_overall,  
      monthly_minutes_used                                  AS used,
      purchased_minutes_used                                AS used_purchased, 
      limit,
      purchased_minutes                                     AS limit_purchased,
      monthly_minutes::VARCHAR                              AS limit_based_plan,
      status,
      status_purchased,
      status_based_plan
    FROM ci_minutes_logic

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2020-12-XX",
    updated_date="2020-12-XX"
) }}