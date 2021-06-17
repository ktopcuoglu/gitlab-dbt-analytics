WITH users AS ( -- active, non-bot users

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_source') }}

), members AS ( -- direct group and project members

    SELECT *
    FROM {{ ref('gitlab_dotcom_members_source') }}
    WHERE user_id IS NOT NULL
      AND is_currently_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
          user_id, 
          source_id, 
          member_source_type 
        ORDER BY 
          access_level DESC, 
          invite_created_at DESC
        ) = 1 
    
), namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces_source') }}

), namespace_lineage AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_lineage_prep') }}

), projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_source') }}

), group_group_links AS ( -- groups invited to groups

    SELECT *
    FROM {{ ref('gitlab_dotcom_group_group_links_source') }}
    WHERE is_currently_valid = TRUE

), project_group_links AS ( -- groups invited to projects

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_group_links_source') }}
    WHERE is_currently_valid = TRUE

), group_group_links_lineage AS (

    SELECT
      group_group_links.shared_group_id, -- the "host" group
      group_group_links.group_group_link_id,
      group_group_links.shared_with_group_id, -- the "guest" group
      group_group_links.group_access,
      namespace_lineage.upstream_lineage  AS base_and_ancestors -- all parent namespaces for the "guest" group
    FROM group_group_links
    INNER JOIN namespace_lineage
      ON group_group_links.shared_with_group_id = namespace_lineage.namespace_id

), project_group_links_lineage AS (

    SELECT
      projects.namespace_id               AS shared_group_id, -- the "host" group the project directly belongs to
      project_group_links.project_group_link_id,
      project_group_links.group_id        AS shared_with_group_id, -- the "guest" group
      project_group_links.group_access,
      namespace_lineage.upstream_lineage  AS base_and_ancestors -- all parent namespaces for the "guest" group
    FROM project_group_links
    INNER JOIN projects
      ON project_group_links.project_id = projects.project_id
    INNER JOIN namespace_lineage
      ON project_group_links.group_id = namespace_lineage.namespace_id

), group_group_links_flattened AS ( -- creates one row for each "guest" group and its parent namespaces

    SELECT
      group_group_links_lineage.*,
      f.value                             AS shared_with_group_lineage
    FROM group_group_links_lineage,
      TABLE(FLATTEN(group_group_links_lineage.base_and_ancestors)) f

), project_group_links_flattened AS ( -- creates one row for each "guest" group and its parent namespaces

    SELECT
      project_group_links_lineage.*,
      f.value                             AS shared_with_group_lineage
    FROM project_group_links_lineage,
      TABLE(FLATTEN(project_group_links_lineage.base_and_ancestors)) f

), group_members AS (

    SELECT *
    FROM members
    WHERE member_source_type = 'Namespace'

), project_members AS (

    SELECT
      projects.namespace_id,
      members.*
    FROM members
    INNER JOIN projects
      ON members.source_id = projects.project_id
    WHERE member_source_type = 'Project'

), group_group_link_members AS (

    SELECT *
    FROM group_group_links_flattened
    INNER JOIN group_members
      ON group_group_links_flattened.shared_with_group_lineage = group_members.source_id

), project_group_link_members AS (

    SELECT *
    FROM project_group_links_flattened
    INNER JOIN group_members
      ON project_group_links_flattened.shared_with_group_lineage = group_members.source_id

), individual_namespaces AS (

    SELECT *
    FROM namespaces
    WHERE namespace_type IS NULL

), unioned AS (

    -- direct member of group
    SELECT
      source_id                           AS namespace_id,
      'group_membership'                  AS membership_source_type,
      source_id                           AS membership_source_id,
      access_level,
      NULL                                AS group_access,
      requested_at,
      user_id
    FROM group_members
  
    UNION
  
    -- direct member of project
    SELECT
      namespace_id,
      'project_membership'                AS membership_source_type,
      source_id                           AS membership_source_id,
      access_level,
      NULL                                AS group_access,
      requested_at,
      user_id
    FROM project_members
  
    UNION
  
    -- differentiate "guest" group from its parent namespaces
    SELECT
      shared_group_id                     AS namespace_id,
      IFF(
          shared_with_group_lineage = shared_with_group_id, 
          'group_group_link', 
          'group_group_link_ancestor'
      )                                   AS membership_source_type,
      group_group_link_id                 AS membership_source_id,
      access_level,
      group_access,
      requested_at,
      user_id
    FROM group_group_link_members
  
    UNION
   
    -- differentiate "guest" group from its parent namespaces
    SELECT
      shared_group_id                     AS namespace_id,
      IFF(
          shared_with_group_lineage = shared_with_group_id, 
          'project_group_link', 
          'project_group_link_ancestor'
      )                                   AS membership_source_type,
      project_group_link_id               AS membership_source_id,
      access_level,
      group_access,
      requested_at,
      user_id
    FROM project_group_link_members
    
    UNION
  
    -- individual namespaces, attributes implied by ownership
    SELECT
      namespace_id,
      'individual_namespace'              AS membership_source_type,
      namespace_id                        AS membership_source_id,
      50                                  AS access_level,
      NULL                                AS group_access,
      NULL                                AS requested_at,
      owner_id                            AS user_id
    FROM individual_namespaces
  
), joined AS (

    SELECT
      namespace_lineage.ultimate_parent_id,
      namespace_lineage.ultimate_parent_plan_id,
      namespace_lineage.ultimate_parent_plan_title,
      unioned.*,
      users.state                         AS user_state,
      users.user_type
    FROM unioned
    INNER JOIN namespace_lineage
      ON unioned.namespace_id = namespace_lineage.namespace_id
    INNER JOIN users
      ON unioned.user_id = users.user_id

), final AS (

    SELECT
      ultimate_parent_id,
      ultimate_parent_plan_id,
      ultimate_parent_plan_title,
      namespace_id,
      membership_source_type,
      membership_source_id,
      access_level,
      group_access,
      IFF(access_level < group_access,
          access_level, group_access)     AS group_level_access,
      requested_at,
      user_id,
      user_state,
      user_type,
      IFF(group_level_access <= 10,
          TRUE, FALSE)                    AS is_guest, -- exclude any user with guest access
      IFF(user_state = 'active'
            AND user_type IS NULL
            AND requested_at IS NULL, 
          TRUE, FALSE)                    AS is_active, -- must be active, not a project bot or GitLab bot, and not awaiting access
      IFF(
          (ultimate_parent_plan_title = 'gold'
            AND is_active = TRUE
            AND is_guest = FALSE)
          OR (ultimate_parent_plan_title != 'gold'
            AND is_active = TRUE),
          TRUE, FALSE)                    AS is_billable, -- exclude guests if namespace has gold plan
      IFF(user_state = 'deactivated'
            AND user_type IS NULL,
          TRUE, FALSE)                    AS is_deactivated,
      IFF(user_state = 'blocked'
            AND user_type IS NULL,
          TRUE, FALSE)                    AS is_blocked,
      IFF(user_type = 6,
          TRUE, FALSE)                    AS is_project_bot,
      IFF(user_type IS NOT NULL
            AND user_type != 6,
          TRUE, FALSE)                    AS is_gitlab_bot
    FROM joined
      
)

{{ dbt_audit(
cte_ref="final",
created_by="@ischweickartDD",
updated_by="@ischweickartDD",
created_date="2021-01-07",
updated_date="2021-01-07"
) }}