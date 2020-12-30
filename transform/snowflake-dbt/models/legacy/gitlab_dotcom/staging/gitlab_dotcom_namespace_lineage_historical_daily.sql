{{ config({
    "materialized": "incremental",
    "unique_key": "snapshot_day_namespace_id"
    })
}}

WITH namespace_snapshots_daily AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_historical_daily') }}
    WHERE snapshot_day >= '2020-01-01'::DATE
    {% if is_incremental() %}

      AND snapshot_day > (select max(snapshot_day) from {{ this }})

    {% endif %}

),  namespace_subscription_snapshot AS (

  SELECT
    *,
    IFNULL(valid_to, CURRENT_TIMESTAMP) AS valid_to_
  FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}

), recursive_namespace_ultimate(snapshot_day, namespace_id, parent_id, upstream_lineage) AS (
    
   SELECT
     snapshot_day,
     namespace_snapshots_daily.namespace_id,
     namespace_snapshots_daily.parent_id,
     TO_ARRAY(namespace_id)                                      AS upstream_lineage
   FROM namespace_snapshots_daily
   WHERE parent_id IS NULL
  
   UNION ALL
  
   SELECT
     iter.snapshot_day,
     iter.namespace_id,
     iter.parent_id,
     ARRAY_INSERT(anchor.upstream_lineage, 0, iter.namespace_id)  AS upstream_lineage
   FROM recursive_namespace_ultimate AS anchor
   INNER JOIN namespace_snapshots_daily AS iter
     ON iter.parent_id = anchor.namespace_id
     AND iter.snapshot_day = anchor.snapshot_day
  
), namespace_lineage_daily AS (
    
    SELECT
      recursive_namespace_ultimate.*,
      upstream_lineage[ARRAY_SIZE(upstream_lineage) - 1]                                AS ultimate_parent_id,
      COALESCE((ultimate_parent_id IN {{ get_internal_parent_namespaces() }}), FALSE)   AS namespace_is_internal,
      CASE
        WHEN namespace_subscription_snapshot.is_trial
          THEN 'trial'
        ELSE COALESCE(namespace_subscription_snapshot.plan_id, 34)::VARCHAR
      END                                                                               AS ultimate_parent_plan_id

    FROM recursive_namespace_ultimate
    LEFT JOIN namespace_subscription_snapshot
      ON upstream_lineage[ARRAY_SIZE(upstream_lineage) - 1] = namespace_subscription_snapshot.namespace_id
      AND recursive_namespace_ultimate.snapshot_day BETWEEN namespace_subscription_snapshot.valid_from::DATE AND namespace_subscription_snapshot.valid_to_::DATE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY recursive_namespace_ultimate.namespace_id, snapshot_day ORDER BY valid_to_ DESC) = 1

)

SELECT
  {{ dbt_utils.surrogate_key(['snapshot_day', 'namespace_id'] ) }}         AS snapshot_day_namespace_id,
  *
FROM namespace_lineage_daily
