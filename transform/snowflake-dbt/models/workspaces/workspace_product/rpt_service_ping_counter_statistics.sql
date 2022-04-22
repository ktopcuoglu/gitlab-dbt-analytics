{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
  ('mart_service_ping_instance_metric', 'mart_service_ping_instance_metric'),
  ('dim_gitlab_releases', 'dim_gitlab_releases')
  ])
}}

-- find min and max version for each metric

, transformed AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['metrics_path', 'ping_edition']) }}                                                   AS rpt_service_ping_counter_statistics_id,
      metrics_path                                                                                                      AS metrics_path,
      ping_edition                                                                                                      AS ping_edition,
      FIRST_VALUE(mart_service_ping_instance_metric.major_minor_version) OVER (
        PARTITION BY metrics_path
          ORDER BY release_date ASC
      )                                                                                                                 AS first_version_with_counter,
      MIN(mart_service_ping_instance_metric.major_version) OVER (
        PARTITION BY metrics_path
      )                                                                                                                 AS first_major_version_with_counter,
      FIRST_VALUE(mart_service_ping_instance_metric.minor_version) OVER (
        PARTITION BY metrics_path
          ORDER BY release_date ASC
      )                                                                                                                 AS first_minor_version_with_counter,
      LAST_VALUE(mart_service_ping_instance_metric.major_minor_version) OVER (
        PARTITION BY metrics_path
          ORDER BY release_date ASC
      )                                                                                                                 AS last_version_with_counter,
      MAX(mart_service_ping_instance_metric.major_version) OVER (
        PARTITION BY metrics_path
      )                                                                                                                 AS last_major_version_with_counter,
      LAST_VALUE(mart_service_ping_instance_metric.minor_version) OVER (
        PARTITION BY metrics_path
          ORDER BY release_date ASC
      )                                                                                                                 AS last_minor_version_with_counter,
      COUNT(DISTINCT dim_installation_id) OVER (PARTITION BY metrics_path)                                              AS dim_installation_count,
      IFF(first_version_with_counter = last_version_with_counter, true, false)                                          AS diff_version_flag
    FROM mart_service_ping_instance_metric
    LEFT JOIN dim_gitlab_releases
      ON mart_service_ping_instance_metric.major_minor_version = dim_gitlab_releases.major_minor_version
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
