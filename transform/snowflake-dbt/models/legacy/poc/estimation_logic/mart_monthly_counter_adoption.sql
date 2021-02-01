WITH paid_subscriptions_monthly_usage_ping_optin AS (

    SELECT *
    FROM {{ ref('mart_paid_subscriptions_monthly_usage_ping_optin') }}

), gitlab_releases AS (
    
    SELECT *
    FROM {{ ref('gitlab_release_schedule') }}

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
      COUNT(DISTINCT subscription_name_slugify)                         AS major_minor_version_subscriptions,
      major_minor_version_subscriptions /  MAX(total_subscrption_count) AS pct_major_minor_version_subscriptions
    FROM paid_subscriptions_monthly_usage_ping_optin
    INNER JOIN {{ ref('gitlab_release_schedule') }} AS gitlab_releases
      ON paid_subscriptions_monthly_usage_ping_optin.latest_major_minor_version = gitlab_releases.major_minor_version
    LEFT JOIN agg_total_subscriptions AS agg ON paid_subscriptions_monthly_usage_ping_optin.reporting_month = agg.agg_month
    {{ dbt_utils.group_by(n=4) }}

), section_metrics AS (
  
    SELECT *
    FROM {{ ref('sheetload_usage_ping_metrics_sections') }}

), flattened_usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data') }},
      lateral flatten(input => version_usage_data.raw_usage_data_payload, recursive => True) f

), transformed_flattened AS (
  
    SELECT DISTINCT
      path                                                                        AS metrics_path, 
      IFF(edition='CE', edition, 'EE')                                            AS edition,
      SPLIT_PART(path, '.', 1)                                                    AS main_json_name,
      SPLIT_PART(path, '.', -1)                                                   AS feature_name,
      REPLACE(path, '.', '_')                                                     AS full_metrics_path,
      major_minor_version,
      major_version,
      minor_version,
      COUNT(DISTINCT host_id || uuid) OVER (
        PARTITION BY path, IFF(edition='CE', edition, 'EE'), major_minor_version) AS installations_with_counter,
      COUNT(DISTINCT CASE WHEN TRY_TO_DECIMAL(value::TEXT) = 0 THEN host_id || uuid END) OVER (
        PARTITION BY path, IFF(edition='CE', edition, 'EE'), major_minor_version) AS installations_with_zero_counter,
      COUNT(DISTINCT host_id || uuid) OVER (
        PARTITION BY IFF(edition='CE', edition, 'EE'), major_minor_version)       AS total_installations
    FROM flattened_usage_data
    WHERE TRY_TO_DECIMAL(value::TEXT) >= 0
      -- Removing SaaS
      AND uuid <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version NOT LIKE '%pre'
  
), counter_threshold AS (

    SELECT *
    FROM transformed_flattened
    WHERE installations_with_counter / total_installations > 0.9
      AND installations_with_zero_counter / total_installations < 0.99

), counter_data AS (
  
    SELECT DISTINCT
      FIRST_VALUE(gitlab_releases.major_version) OVER (PARTITION BY counter_threshold.metrics_path, edition
                                        ORDER BY release_date ASC)                                 AS major_version,
      FIRST_VALUE(gitlab_releases.major_version) OVER (PARTITION BY counter_threshold.metrics_path, edition
                                         ORDER BY release_date ASC)                                AS minor_version,
      FIRST_VALUE(DATE_TRUNC('month', release_date)) OVER (PARTITION BY counter_threshold.metrics_path, edition 
                                                            ORDER BY counter_threshold.major_version ASC, 
                                                              counter_threshold.minor_version ASC) AS release_month,
      counter_threshold.metrics_path,
      stage_name, 
      section_name,
      group_name,
      is_smau,
      is_gmau,
      is_umau,
      is_paid_gmau,
      edition,
      FIRST_VALUE(counter_threshold.major_minor_version) OVER (PARTITION BY counter_threshold.metrics_path, edition 
                                              ORDER BY counter_threshold.major_version ASC, 
                                                        counter_threshold.minor_version ASC)       AS first_version_with_counter
    FROM counter_threshold
    INNER JOIN section_metrics ON counter_threshold.metrics_path = section_metrics.metrics_path
    LEFT JOIN gitlab_releases ON counter_threshold.major_minor_version = gitlab_releases.major_minor_version
    WHERE release_date < CURRENT_DATE

), date_spine AS (
    
    SELECT DISTINCT first_day_of_month AS reporting_month
    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < CURRENT_DATE
      AND first_day_of_month >= '2018-01-01'

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
        AND (counter_data.major_version < monthly_subscription_optin_counts.major_version OR
        (counter_data.major_version = monthly_subscription_optin_counts.major_version AND counter_data.minor_version <= monthly_subscription_optin_counts.minor_version))
    WHERE date_spine.reporting_month < DATE_TRUNC('month', CURRENT_DATE)
    {{ dbt_utils.group_by(n=11) }}
  
)
  
SELECT *
FROM date_joined

