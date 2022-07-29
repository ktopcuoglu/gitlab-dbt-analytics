{{
  simple_cte([
    ('repositories','gitlab_dotcom_container_repositories_source'),
    ('projects','gitlab_dotcom_projects_source'),
    ('namespaces','gitlab_dotcom_namespaces_source')
  ])
}},

full_namespace_path(namespace_id, namespace_path, parent_id, namespace_root, full_path) AS (

  SELECT
    namespace_id,
    namespace_path,
    parent_id,
    namespace_path AS namespace_root,
    namespace_path AS full_path
  FROM namespaces AS root
  WHERE parent_id IS NULL

  UNION ALL

  SELECT
    iteration.namespace_id,
    iteration.namespace_path,
    iteration.parent_id,
    anchor.namespace_root,
    anchor.full_path || '/' || iteration.namespace_path AS full_path
  FROM full_namespace_path as anchor
  INNER JOIN namespaces AS iteration
    ON anchor.namespace_id = iteration.parent_id

),

full_container_path AS (

  SELECT
    repositories.container_repository_id,
    projects.project_id,
    full_namespace_path.namespace_id,
    full_namespace_path.parent_id,
    full_namespace_path.full_path,
    projects.project_path,
    repositories.container_repository_name,
    full_namespace_path.namespace_root,
    LOWER(full_namespace_path.full_path) || '/' ||
    LOWER(projects.project_path) || 
    COALESCE('/' || LOWER(repositories.container_repository_name),'') AS container_path
  FROM repositories
  LEFT JOIN projects
    ON repositories.project_id = projects.project_id
  LEFT JOIN full_namespace_path
    ON projects.namespace_id = full_namespace_path.namespace_id

)

SELECT *
FROM full_container_path
