/* grain: one record per host per metric per month */
{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([('flattened_usage_data', 'prep_usage_data_flattened'),
                ('fct_usage_ping_payload', 'fct_usage_ping_payload'),
                ('dim_gitlab_releases', 'dim_gitlab_releases'),
                ]
                )
}}
                
, transformed AS (
  
    SELECT DISTINCT
      metrics_path, 
      IFF(fct_usage_ping_payload.edition='CE', edition, 'EE') AS edition,
      SPLIT_PART(metrics_path, '.', 1)                        AS main_json_name,
      SPLIT_PART(metrics_path, '.', -1)                       AS feature_name,
      FIRST_VALUE(fct_usage_ping_payload.major_minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS first_version_with_counter,
      MIN(fct_usage_ping_payload.major_version) OVER (
        PARTITION BY metrics_path
      )                                                       AS first_major_version_with_counter,
      FIRST_VALUE(fct_usage_ping_payload.minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS first_minor_version_with_counter,
      LAST_VALUE(fct_usage_ping_payload.major_minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS last_version_with_counter,
      MAX(fct_usage_ping_payload.major_version) OVER (
        PARTITION BY metrics_path
      )                                                       AS last_major_version_with_counter,
      LAST_VALUE(fct_usage_ping_payload.minor_version) OVER (
        PARTITION BY metrics_path 
        ORDER BY release_date ASC
      )                                                       AS last_minor_version_with_counter,
      COUNT(DISTINCT dim_instance_id) OVER (PARTITION BY metrics_path)    AS count_instances
    FROM flattened_usage_data
    LEFT JOIN fct_usage_ping_payload 
      ON flattened_usage_data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id 
    LEFT JOIN dim_gitlab_releases
      ON fct_usage_ping_payload.major_minor_version = dim_gitlab_releases.major_minor_version
    WHERE TRY_TO_DECIMAL(metric_value::TEXT) > 0
      -- Removing SaaS
      AND dim_instance_id <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version_is_prerelease = FALSE

)

SELECT *
FROM transformed
