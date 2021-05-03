{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('namespace_current', 'gitlab_dotcom_namespaces_source'),
    ('namespace_snapshots', 'gitlab_dotcom_namespaces_snapshots_base'),
    ('namespace_lineage_current', 'gitlab_dotcom_namespace_lineage_prep'),
    ('map_namespace_internal', 'map_namespace_internal'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('product_tiers', 'prep_product_tier'),
    ('members_source', 'gitlab_dotcom_members_source'),
    ('projects_source', 'gitlab_dotcom_projects_source'),
    ('audit_events', 'gitlab_dotcom_audit_events_source'),
    ('audit_event_details_clean', 'gitlab_dotcom_audit_event_details_clean')
]) }}

, saas_product_tiers AS (

  SELECT *
  FROM product_tiers
  WHERE product_delivery_type = 'SaaS'

), members AS (

    SELECT
      source_id,
      COUNT(DISTINCT member_id)                                                       AS member_count
    FROM members_source
    WHERE is_currently_valid = TRUE
      AND member_source_type = 'Namespace'
      AND {{ filter_out_blocked_users('members_source', 'user_id') }}
    GROUP BY 1

), projects AS (

    SELECT
      namespace_id,
      COUNT(DISTINCT project_id)                                                      AS project_count
    FROM projects_source
    GROUP BY 1

), creators AS (

    SELECT
      author_id                                                                       AS creator_id,
      entity_id                                                                       AS group_id
    FROM audit_events
    INNER JOIN audit_event_details_clean
      ON audit_events.audit_event_id = audit_event_details_clean.audit_event_id
    WHERE entity_type = 'Group'
      AND key_name = 'add'
      AND key_value = 'group'
    GROUP BY 1, 2

), namespace_historical AS (

    SELECT *
    FROM namespace_snapshots
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY namespace_id
      ORDER BY valid_from DESC
    ) = 1

), namespaces AS (

    SELECT
      namespace_historical.*,
      IFNULL(namespace_current.namespace_id IS NOT NULL, FALSE)                       AS is_currently_valid
    FROM namespace_historical
    LEFT OUTER JOIN namespace_current
      ON namespace_historical.namespace_id = namespace_current.namespace_id

), namespace_lineage AS (

  SELECT
    namespace_lineage_current.*,
    plans.plan_name                                                                   AS ultimate_parent_plan_name
    FROM namespace_lineage_current
    INNER JOIN plans
      ON namespace_lineage_current.ultimate_parent_plan_id = plans.plan_id

), joined AS (

    SELECT
      namespaces.namespace_id                                                         AS dim_namespace_id,
      IFNULL(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL, FALSE)  AS namespace_is_internal,
      IFNULL(namespace_lineage.ultimate_parent_id = namespaces.namespace_id, FALSE)   AS namespace_is_ultimate_parent,
      CASE
        WHEN namespaces.visibility_level = 'public'
          OR namespace_is_internal                    THEN namespace_name
        WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
        WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END                                                                             AS namespace_name,
      CASE
       WHEN namespaces.visibility_level = 'public'
         OR namespace_is_internal                     THEN namespace_path
       WHEN namespaces.visibility_level = 'internal'  THEN 'internal - masked'
       WHEN namespaces.visibility_level = 'private'   THEN 'private - masked'
      END                                                                             AS namespace_path,
      namespaces.owner_id,
      IFNULL(namespaces.namespace_type, 'Individual')                                 AS namespace_type,
      namespaces.has_avatar,
      namespaces.namespace_created_at,
      namespaces.namespace_updated_at,
      namespaces.is_membership_locked,
      namespaces.has_request_access_enabled,
      namespaces.has_share_with_group_locked,
      namespaces.visibility_level,
      namespaces.ldap_sync_status,
      namespaces.ldap_sync_error,
      namespaces.ldap_sync_last_update_at,
      namespaces.ldap_sync_last_successful_update_at,
      namespaces.ldap_sync_last_sync_at,
      namespaces.lfs_enabled,
      namespaces.parent_id,
      namespaces.shared_runners_enabled,
      namespaces.shared_runners_minutes_limit,
      namespaces.extra_shared_runners_minutes_limit,
      namespaces.repository_size_limit,
      namespaces.does_require_two_factor_authentication,
      namespaces.two_factor_grace_period,
      namespaces.project_creation_level,
      namespaces.push_rule_id,
      IFNULL(creators.creator_id, namespaces.owner_id)                                  AS creator_id,
      namespace_lineage.ultimate_parent_id                                              AS ultimate_parent_namespace_id,
      namespace_lineage.ultimate_parent_plan_id                                         AS gitlab_plan_id,
      namespace_lineage.ultimate_parent_plan_title                                      AS gitlab_plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid                                    AS gitlab_plan_is_paid,
      {{ get_keyed_nulls('saas_product_tiers.dim_product_tier_id') }}                   AS dim_product_tier_id,
      IFNULL(members.member_count, 0)                                                   AS current_member_count,
      IFNULL(projects.project_count, 0)                                                 AS current_project_count,
      namespaces.is_currently_valid
    FROM namespaces
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id
      LEFT JOIN members
        ON namespaces.namespace_id = members.source_id
      LEFT JOIN projects
        ON namespaces.namespace_id = projects.namespace_id
      LEFT JOIN creators
        ON namespaces.namespace_id = creators.group_id
      LEFT JOIN map_namespace_internal
        ON namespace_lineage.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
      LEFT JOIN saas_product_tiers
        ON namespace_lineage.ultimate_parent_plan_name = LOWER(IFF(saas_product_tiers.product_tier_name_short != 'Trial: Ultimate',
                                                                    saas_product_tiers.product_tier_historical_short,
                                                                    'ultimate_trial'))

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-14",
    updated_date="2021-04-29"
) }}