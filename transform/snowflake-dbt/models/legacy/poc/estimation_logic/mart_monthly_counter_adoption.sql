WITH paid_subscriptions_monthly_usage_ping_optin AS (

    SELECT *
    FROM {{ ref('mart_paid_subscriptions_monthly_usage_ping_optin') }}

), gitlab_releases AS (
    
    SELECT *
    FROM {{ ref('dim_gitlab_releases') }}

), agg_total_subscriptions AS (

    SELECT 
      reporting_month AS agg_month,
      COUNT(DISTINCT subscription_name_slugify) AS total_subscrption_count
    FROM paid_subscriptions_monthly_usage_ping_optin
    GROUP BY 1

), monthly_subscription_optin_counts AS (

    SELECT
      paid_subscriptions_monthly_usage_ping_optin.reporting_month,
      latest_major_minor_version,
      major_version,
      minor_version,
      release_date,
      DATE_TRUNC('month', release_date)                                AS release_month,
      COUNT(DISTINCT subscription_name_slugify)                         AS major_minor_version_subscriptions,
      major_minor_version_subscriptions /  MAX(total_subscrption_count) AS pct_major_minor_version_subscriptions
    FROM paid_subscriptions_monthly_usage_ping_optin
    INNER JOIN gitlab_releases
      ON paid_subscriptions_monthly_usage_ping_optin.latest_major_minor_version = gitlab_releases.major_minor_version
    LEFT JOIN agg_total_subscriptions AS agg ON paid_subscriptions_monthly_usage_ping_optin.reporting_month = agg.agg_month
    {{ dbt_utils.group_by(n=6) }}

), section_metrics AS (
  
    SELECT *
    FROM {{ ref('sheetload_usage_ping_metrics_sections') }}

), flattened_usage_data AS (

    SELECT *
    FROM {{ ref('prep_usage_data_flattened') }}

), transformed_flattened AS (
  
    SELECT DISTINCT
      metrics_path, 
      IFF(edition='CE', edition, 'EE')                                      AS edition,
      SPLIT_PART(metrics_path, '.', 1)                                      AS main_json_name,
      SPLIT_PART(metrics_path, '.', -1)                                     AS feature_name,
      REPLACE(metrics_path, '.', '_')                                       AS full_metrics_path,
      FIRST_VALUE(flattened_usage_data.major_minor_version ) OVER (PARTITION BY metrics_path, edition 
                                                ORDER BY release_date ASC) AS first_version_with_counter
    FROM flattened_usage_data
    LEFT JOIN gitlab_releases ON flattened_usage_data.major_minor_version = gitlab_releases.major_minor_version
    WHERE TRY_TO_DECIMAL(metric_value::TEXT) > 0
      -- Removing SaaS
      AND instance_id <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version NOT LIKE '%pre'
  
), counter_data AS (
  
    SELECT DISTINCT
      FIRST_VALUE(major_version) OVER (PARTITION BY transformed_flattened.metrics_path, edition
                                        ORDER BY release_date ASC)                           AS major_version,
      FIRST_VALUE(minor_version) OVER (PARTITION BY transformed_flattened.metrics_path, edition
                                         ORDER BY release_date ASC)                          AS minor_version,
      FIRST_VALUE(DATE_TRUNC('month', release_date)) OVER (PARTITION BY transformed_flattened.metrics_path, edition ORDER BY
                                 major_version ASC, minor_version ASC)                       AS release_month,
      transformed_flattened.metrics_path,
      stage_name, 
      section_name,
      group_name,
      is_smau,
      is_gmau,
      is_umau,
      is_paid_gmau,
      edition,
      FIRST_VALUE(major_minor_version) OVER (PARTITION BY transformed_flattened.metrics_path, edition 
                                              ORDER BY release_date ASC) AS first_version_with_counter
    FROM transformed_flattened
    INNER JOIN section_metrics ON transformed_flattened.metrics_path =  section_metrics.metrics_path
    LEFT JOIN gitlab_releases ON transformed_flattened.first_version_with_counter = gitlab_releases.major_minor_version
    WHERE release_date < CURRENT_DATE

), date_spine AS (
    
    SELECT DISTINCT first_day_of_month AS reporting_month
    FROM {{ ref('date_details') }}
    WHERE first_day_of_month >= '2018-01-01'
      AND first_day_of_month < CURRENT_DATE

), date_joined AS (
  
    SELECT 
      date_spine.reporting_month, 
      first_version_with_counter,
      metrics_path,
      counter_data.edition,
      stage_name, 
      section_name, 
      group_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      SUM(pct_major_minor_version_subscriptions) AS pct_subscriptions_with_counters
    FROM date_spine
    INNER JOIN counter_data
      ON date_spine.reporting_month >= release_month
    LEFT JOIN monthly_subscription_optin_counts
      ON date_spine.reporting_month = monthly_subscription_optin_counts.reporting_month
        AND counter_data.release_month <= monthly_subscription_optin_counts.release_month
    {{ dbt_utils.group_by(n=11) }}
  
)
  
SELECT *
FROM date_joined
