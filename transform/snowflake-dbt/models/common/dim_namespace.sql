WITH map_namespace_internal AS (

    SELECT *
    FROM {{ ref('map_namespace_internal') }}

), prep_product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}

), namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces_source')}}

), members AS (

    SELECT
      members.source_id,
      COUNT(DISTINCT members.member_id) AS member_count
    FROM {{ref('gitlab_dotcom_members_source')}} members
    WHERE is_currently_valid = TRUE
    AND members.member_source_type = 'Namespace'
      AND NOT EXISTS ( SELECT 1
                       FROM {{ref('gitlab_dotcom_users_source')}} users_source
                       WHERE users_source.state = 'blocked'
                         AND users_source.user_id = members.user_id
                     )
    GROUP BY members.source_id

), projects AS (

    SELECT
      projects.namespace_id,
      COUNT(DISTINCT projects.project_id) AS project_count
    FROM {{ref('gitlab_dotcom_projects_source')}} projects
    GROUP BY projects.namespace_id

), namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage_prep')}}

), creators AS (

    SELECT DISTINCT
      author_id AS creator_id,
      entity_id AS group_id
    FROM {{ ref('gitlab_dotcom_audit_events_source') }}        AS audit_events
    JOIN {{ ref('gitlab_dotcom_audit_event_details_clean') }}  AS audit_event_details_clean
      ON audit_event_details_clean.audit_event_id = audit_events.audit_event_id
    WHERE entity_type = 'Group'
      AND key_name = 'add'
      AND key_value = 'group'

), joined AS (

    SELECT
      namespaces.namespace_id                                                           AS dim_namespace_id,
      namespaces.namespace_id,
      COALESCE(map_namespace_internal.ultimate_parent_namespace_id IS NOT NULL, FALSE)  AS namespace_is_internal,
      COALESCE(namespace_lineage.ultimate_parent_id = namespaces.namespace_id, FALSE)   AS namespace_is_ultimate_parent,
      CASE
        WHEN namespaces.visibility_level = 'public' OR namespace_is_internal THEN namespace_name
        WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
        WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END                                                                               AS namespace_name,
      CASE
       WHEN namespaces.visibility_level = 'public' OR namespace_is_internal THEN namespace_path
       WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
       WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END AS namespace_path,
      namespaces.owner_id,
      COALESCE(namespaces.namespace_type, 'Individual')                                 AS namespace_type,
      namespaces.has_avatar,
      namespaces.created_at                                                             AS namespace_created_at,
      namespaces.updated_at                                                             AS namespace_updated_at,
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
      namespaces.shared_runners_minutes_limit,
      namespaces.extra_shared_runners_minutes_limit,
      namespaces.repository_size_limit,
      namespaces.does_require_two_factor_authentication,
      namespaces.two_factor_grace_period,
      namespaces.project_creation_level,
      namespaces.push_rule_id,
      COALESCE(creators.creator_id, namespaces.owner_id)                                AS creator_id,
      namespace_lineage.ultimate_parent_id                                              AS ultimate_parent_namespace_id,
      namespace_lineage.ultimate_parent_plan_id                                         AS gitlab_plan_id,
      namespace_lineage.ultimate_parent_plan_title                                      AS gitlab_plan_title,
      namespace_lineage.ultimate_parent_plan_is_paid                                    AS gitlab_plan_is_paid,
      IFNULL(product_tier_ultimate_parent.dim_product_tier_id,MD5('-1'))                AS dim_product_tier_id,
      COALESCE(member_count, 0)                                                         AS current_member_count,
      COALESCE(project_count, 0)                                                        AS current_project_count

    FROM namespaces
      LEFT JOIN members
        ON namespaces.namespace_id = members.source_id
      LEFT JOIN projects
        ON namespaces.namespace_id = projects.namespace_id
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id
      LEFT JOIN creators
        ON namespaces.namespace_id = creators.group_id
      LEFT JOIN map_namespace_internal
        ON namespace_lineage.ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
      LEFT JOIN prep_product_tier AS product_tier_ultimate_parent
        ON LOWER(product_tier_ultimate_parent.product_tier_name) = LOWER(namespace_lineage.ultimate_parent_plan_title)

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@snalamaru",
    updated_by="@mcooperDD",
    created_date="2020-12-29",
    updated_date="2020-12-31"
) }}
