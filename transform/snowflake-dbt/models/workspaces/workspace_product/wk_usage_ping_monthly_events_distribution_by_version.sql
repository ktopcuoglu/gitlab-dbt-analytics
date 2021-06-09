{{ config({
    "materialized": "incremental",
    "unique_key": "month_version_id"
    })
}}

WITH filtered_counters AS (
  
  SELECT *
  FROM {{ ref('mart_usage_ping_counters_statistics') }}
  WHERE metrics_path ILIKE 'counts.%' AND edition = 'CE'
    AND first_major_version_with_counter BETWEEN 1 AND 12

), monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}
    WHERE monthly_metric_value > 0
      AND metrics_path ILIKE 'counts.%'
      {% if is_incremental() %}

      AND created_month >= (SELECT MAX(reporting_month) FROM {{this}})

      {% endif %}
), dim_gitlab_releases AS (

    SELECT *
    FROM {{ ref('dim_gitlab_releases') }}

), fct_usage_ping_payload AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payload') }}

), outlier_detection_formula AS (

    SELECT 
      created_month AS reporting_month,
      metrics_path,
      (APPROX_PERCENTILE(monthly_metric_value , 0.75 ) -
      APPROX_PERCENTILE(monthly_metric_value , 0.25 )) * 3 + APPROX_PERCENTILE(monthly_metric_value , 0.75 ) AS outer_boundary
    FROM monthly_usage_data
    WHERE monthly_metric_value > 0
      AND metrics_path ILIKE 'counts.%'
      AND created_month >= '2020-01-01'
    GROUP BY 1,2
  
), joined AS (
  
    SELECT 
      product_usage.created_month                                                                                            AS reporting_month, 
      fct_usage_ping_payload.major_minor_version,
      DATEDIFF('month', DATE_TRUNC('month', release_date), product_usage.created_month)                                      AS months_since_release, 
      IFF(fct_usage_ping_payload.edition = 'CE', 'CE', IFF(product_tier = 'Core', 'EE - Core', 'EE - Paid'))                                   AS reworked_main_edition, 
      SUM(monthly_metric_value)                                                                                              AS total_counts
    FROM monthly_usage_data AS product_usage
    LEFT JOIN fct_usage_ping_payload
      ON product_usage.ping_id = fct_usage_ping_payload.dim_usage_ping_id
    LEFT JOIN dim_gitlab_releases AS release 
      ON fct_usage_ping_payload.major_minor_version = release.major_minor_version
    INNER JOIN filtered_counters 
      ON product_usage.metrics_path = filtered_counters.metrics_path
    INNER JOIN outlier_detection_formula 
      ON product_usage.metrics_path = outlier_detection_formula.metrics_path 
      AND product_usage.created_month = outlier_detection_formula.reporting_month
      AND product_usage.monthly_metric_value <= outer_boundary
    WHERE usage_ping_delivery_type = 'Self-Managed'
      AND product_usage.created_month > '2020-01-01'
      AND is_trial = False
    GROUP BY 1,2,3,4
  
), data_with_unique_key AS (

    SELECT
      {{ dbt_utils.surrogate_key(['reporting_month', 
                                  'major_minor_version', 
                                  'reworked_main_edition']) }} AS month_version_id,
      *
    FROM joined

)

SELECT *
FROM data_with_unique_key
