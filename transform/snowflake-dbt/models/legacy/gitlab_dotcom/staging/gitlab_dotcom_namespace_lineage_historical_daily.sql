{{ config({
    "materialized": "incremental",
    "unique_key": "snapshot_day_namespace_id"
    })
}}

{{ simple_cte([
    ('map_namespace_internal', 'map_namespace_internal'),
    ('namespace_subscription_snapshots', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base')
]) }}

, namespace_snapshots_daily AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_historical_daily') }}
    WHERE snapshot_day >= '2020-01-01'::DATE
    {% if is_incremental() %}

      AND snapshot_day > (select max(snapshot_day) from {{ this }})

    {% endif %}

), recursive_namespace_ultimate(snapshot_day, namespace_id, parent_id, upstream_lineage) AS (
    
   SELECT
     snapshot_day,
     namespace_snapshots_daily.namespace_id,
     namespace_snapshots_daily.parent_id,
     TO_ARRAY(namespace_id)                                                           AS upstream_lineage
   FROM namespace_snapshots_daily
   WHERE parent_id IS NULL
  
   UNION ALL
  
   SELECT
     iter.snapshot_day,
     iter.namespace_id,
     iter.parent_id,
     ARRAY_APPEND(anchor.upstream_lineage, iter.namespace_id)                         AS upstream_lineage
   FROM recursive_namespace_ultimate AS anchor
   INNER JOIN namespace_snapshots_daily AS iter
     ON iter.parent_id = anchor.namespace_id
     AND iter.snapshot_day = anchor.snapshot_day
  
), namespace_lineage_daily AS (
    
    SELECT
      recursive_namespace_ultimate.*,
      upstream_lineage[0]::INT                                                        AS ultimate_parent_id
    FROM recursive_namespace_ultimate

), with_plans AS (

    SELECT
      namespace_lineage_daily.*,
      IFNULL(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL, FALSE)  AS namespace_is_internal,
      IFF(namespace_subscription_snapshots.is_trial
            AND IFNULL(namespace_subscription_snapshots.plan_id, 34) <> 34,
          102, IFNULL(namespace_subscription_snapshots.plan_id, 34))                  AS ultimate_parent_plan_id
    FROM namespace_lineage_daily
    LEFT JOIN namespace_subscription_snapshots
      ON namespace_lineage_daily.ultimate_parent_id = namespace_subscription_snapshots.namespace_id
      AND namespace_lineage_daily.snapshot_day BETWEEN namespace_subscription_snapshots.valid_from::DATE AND IFNULL(namespace_subscription_snapshots.valid_to::DATE, CURRENT_DATE)
    LEFT JOIN map_namespace_internal
      ON namespace_lineage_daily.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY
        namespace_lineage_daily.namespace_id,
        snapshot_day
      ORDER BY valid_from DESC
      ) = 1

)

SELECT
  {{ dbt_utils.surrogate_key(['snapshot_day', 'namespace_id'] ) }}                    AS snapshot_day_namespace_id,
  *
FROM namespace_lineage_daily