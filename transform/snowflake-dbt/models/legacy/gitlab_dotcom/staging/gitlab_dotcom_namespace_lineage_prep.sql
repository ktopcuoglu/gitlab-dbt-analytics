{{ simple_cte([
    ('namespaces', 'gitlab_dotcom_namespaces_source'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_source')
]) }}

, active_gitlab_subscriptions AS (

    SELECT *
    FROM gitlab_subscriptions
    WHERE is_currently_valid = TRUE
      AND IFNULL(gitlab_subscription_end_date, CURRENT_DATE) >= CURRENT_DATE

), all_namespaces AS (

    SELECT
      namespace_id,
      parent_id
    FROM namespaces

    UNION ALL
    /*
      Union parent_ids with deleted namespace_ids. These cause their child namespaces to be missed by the top-down recursive CTE.
      Child namespaces with deleted parents are quite rare (n=82 on 2020-01-06, n=113 on 2020-12-17, n=114 on 2021-05-01),
      but need to be included in this model for full coverage.
    */
    SELECT
      deleted_parents.parent_id                                                 AS namespace_id,
      NULL                                                                      AS parent_id
    FROM namespaces deleted_parents
    LEFT JOIN namespaces ultimate_parents
      ON deleted_parents.parent_id = ultimate_parents.namespace_id
    WHERE deleted_parents.parent_id IS NOT NULL
      AND ultimate_parents.namespace_id IS NULL
    GROUP BY 1,2

), recursive_namespaces(namespace_id, parent_id, upstream_lineage) AS (

  -- Select all namespaces without parents
    SELECT
      namespace_id,
      parent_id,
      TO_ARRAY(namespace_id)                                                    AS upstream_lineage -- Initiate lineage array
    FROM all_namespaces
    WHERE parent_id IS NULL

    UNION ALL

    -- Recursively iterate through each of the children namespaces 
  
    SELECT
      iter.namespace_id,
      iter.parent_id,
      ARRAY_APPEND(anchor.upstream_lineage, iter.namespace_id)                  AS upstream_lineage -- Copy the lineage array of parent, append self to end
    FROM recursive_namespaces AS anchor -- Parent namespace
    INNER JOIN all_namespaces AS iter -- Child namespace
      ON anchor.namespace_id = iter.parent_id

), extracted AS (

    SELECT
      *,
      upstream_lineage[0]::INT                                                  AS ultimate_parent_id -- First item is the ultimate parent.
    FROM recursive_namespaces
    WHERE namespace_id != 0
  
), with_plans AS (

    SELECT
      extracted.*,
      namespace_plans.plan_id                                                   AS namespace_plan_id,
      namespace_plans.plan_title                                                AS namespace_plan_title,
      namespace_plans.plan_is_paid                                              AS namespace_plan_is_paid,
      IFF(ultimate_parent_gitlab_subscriptions.is_trial
            AND IFNULL(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34,
          102, IFNULL(ultimate_parent_plans.plan_id, 34))                       AS ultimate_parent_plan_id,
      IFF(ultimate_parent_gitlab_subscriptions.is_trial
            AND IFNULL(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34,
          'Ultimate Trial', IFNULL(ultimate_parent_plans.plan_title, 'Free'))   AS ultimate_parent_plan_title,
      IFF(ultimate_parent_gitlab_subscriptions.is_trial
            AND IFNULL(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34,
          FALSE, IFNULL(ultimate_parent_plans.plan_is_paid, FALSE))             AS ultimate_parent_plan_is_paid
    FROM extracted
    -- Get plan information for the namespace.
    LEFT JOIN active_gitlab_subscriptions AS namespace_gitlab_subscriptions
      ON extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS namespace_plans
      ON IFNULL(namespace_gitlab_subscriptions.plan_id, 34) = namespace_plans.plan_id
    -- Get plan information for the ultimate parent namespace.
    LEFT JOIN active_gitlab_subscriptions AS ultimate_parent_gitlab_subscriptions
      ON extracted.ultimate_parent_id = ultimate_parent_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS ultimate_parent_plans
      ON IFNULL(ultimate_parent_gitlab_subscriptions.plan_id, 34) = ultimate_parent_plans.plan_id

)
SELECT *
FROM with_plans