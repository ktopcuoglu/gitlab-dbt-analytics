{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
  ('mart_ping_instance_metric', 'mart_ping_instance_metric'),
  ('dim_gitlab_releases', 'dim_gitlab_releases')
  ])
}}

-- find min and max version for each metric

, transformed AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['metrics_path', 'ping_edition']) }}                                                   AS rpt_ping_counter_statistics_id,
      metrics_path                                                                                                      AS metrics_path,
      ping_edition                                                                                                      AS ping_edition,
      -- Grab first major/minor edition where metric/edition was present
      FIRST_VALUE(mart_ping_instance_metric.major_minor_version_id) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS first_major_minor_version_id_with_counter,
      -- Grab first major/minor edition where metric/edition was present
      FIRST_VALUE(mart_ping_instance_metric.major_minor_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS first_major_minor_version_with_counter,
      -- Grab first major edition where metric/edition was present
      FIRST_VALUE(mart_ping_instance_metric.major_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS first_major_version_with_counter,
      -- Grab first minor edition where metric/edition was present
      FIRST_VALUE(mart_ping_instance_metric.minor_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS first_minor_version_with_counter,
      -- Grab last major/minor edition where metric/edition was present
      LAST_VALUE(mart_ping_instance_metric.major_minor_version_id) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS last_major_minor_version_id_with_counter,
      -- Grab last major/minor edition where metric/edition was present
      LAST_VALUE(mart_ping_instance_metric.major_minor_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS last_major_minor_version_with_counter,
      -- Grab last major edition where metric/edition was present
      LAST_VALUE(mart_ping_instance_metric.major_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS last_major_version_with_counter,
      -- Grab last minor edition where metric/edition was present
      LAST_VALUE(mart_ping_instance_metric.minor_version) OVER (
        PARTITION BY metrics_path, ping_edition
          ORDER BY major_minor_version_id ASC
      )                                                                                                                 AS last_minor_version_with_counter,
      -- Get count of installations per each metric/edition
      COUNT(DISTINCT dim_installation_id) OVER (PARTITION BY metrics_path, ping_edition)                                AS dim_installation_count
    FROM mart_ping_instance_metric
      INNER JOIN dim_gitlab_releases --limit to valid versions
          ON mart_ping_instance_metric.major_minor_version = dim_gitlab_releases.major_minor_version
    WHERE --TRY_TO_DECIMAL(metric_value::TEXT) > 0
      -- Removing SaaS
      dim_instance_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version_is_prerelease = FALSE

)

{{ dbt_audit(
    cte_ref="transformed",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-04-07",
    updated_date="2022-04-15"
) }}
