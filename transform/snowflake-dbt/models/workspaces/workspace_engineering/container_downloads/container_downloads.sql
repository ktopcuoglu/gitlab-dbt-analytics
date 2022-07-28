{{
  config(
    materialized='table'
  )
}}

{{
  simple_cte([
    ('ip_ranges','valid_google_service_ips'),
    ('namespace_map','container_namespace_map'),
    ('container_downloads','container_registry_download_logs_source'),
    ('namespaces','dim_namespace')
  ])
}},

add_gcp_source AS (

  SELECT
    container_downloads.correlation_id,
    container_downloads.downloaded_at,
    container_downloads.root_repository,
    container_downloads.container_path,
    container_downloads.download_size_bytes,
    IFF(ip_ranges.scope IS NULL, 'Non-GCP', ip_ranges.scope) AS gcp_scope
  FROM container_downloads
  LEFT JOIN ip_ranges
    ON container_downloads.downloaded_by_hex_ip >= ip_ranges.hex_ip_range_start
      AND container_downloads.downloaded_by_hex_ip <= ip_ranges.hex_ip_range_end

),

agg_download_data AS (

  SELECT
    DATE_TRUNC('day', add_gcp_source.downloaded_at)::DATE AS downloaded_date,
    add_gcp_source.root_repository,
    add_gcp_source.container_path,
    add_gcp_source.gcp_scope,
    COUNT(add_gcp_source.correlation_id) AS download_count,
    SUM(add_gcp_source.download_size_bytes) AS total_bytes,
    AVG(add_gcp_source.download_size_bytes) AS avg_bytes,
    total_bytes / 1000 / 1000 / 1000 AS total_gb,
    total_bytes / 1024 / 1024 / 1024 AS total_gib,
    total_gib * 0.066 AS est_cost
  FROM add_gcp_source
  GROUP BY 1, 2, 3, 4

),

report AS (

  SELECT
    agg_download_data.downloaded_date,
    namespace_map.container_repository_id,
    namespace_map.project_id,
    namespaces.dim_namespace_id AS namespace_id,
    namespaces.namespace_path,
    CASE
       WHEN namespaces.visibility_level = 'public'
         OR namespaces.namespace_is_internal          THEN namespace_map.container_path
       WHEN namespaces.visibility_level = 'internal'  THEN 'internal - masked'
       WHEN namespaces.visibility_level = 'private'   THEN 'private - masked'
    END AS container_path,
    ultimate_parent_namespace.dim_namespace_id AS ultimate_parent_id,
    ultimate_parent_namespace.namespace_name AS ultimate_parent_name,
    ultimate_parent_namespace.namespace_path AS ultimate_parent_path,
    namespaces.namespace_type,
    namespaces.gitlab_plan_id,
    CASE
      WHEN ultimate_parent_namespace.namespace_is_internal THEN 'Gitlab Internal'
      WHEN namespaces.gitlab_plan_is_paid = TRUE THEN 'Paid'
      WHEN namespaces.gitlab_plan_is_paid = FALSE THEN 'Free'
    END AS gitlab_plan_type,
    namespaces.gitlab_plan_title,
    namespaces.gitlab_plan_is_paid,
    namespaces.creator_id,
    namespaces.current_member_count,
    namespaces.current_project_count,
    agg_download_data.download_count,
    agg_download_data.gcp_scope,
    agg_download_data.total_bytes,
    agg_download_data.avg_bytes,
    agg_download_data.total_gb,
    agg_download_data.total_gib,
    agg_download_data.est_cost
  FROM agg_download_data
  LEFT JOIN namespace_map
    ON agg_download_data.container_path = namespace_map.container_path
  LEFT JOIN namespaces
    ON namespace_map.namespace_id = namespaces.dim_namespace_id
  LEFT JOIN namespaces AS ultimate_parent_namespace
    ON namespaces.ultimate_parent_namespace_id = ultimate_parent_namespace.dim_namespace_id

)

SELECT *
FROM report
