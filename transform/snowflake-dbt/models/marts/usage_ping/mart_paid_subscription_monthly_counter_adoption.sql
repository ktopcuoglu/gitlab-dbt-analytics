WITH self_managed_active_subscriptions AS (

    SELECT
      dim_date_id           AS date_id,
      dim_subscription_id   AS subscription_id,
      dim_product_detail_id AS product_details_id,
      SUM(mrr)              AS mrr,
      SUM(quantity)         AS quantity
    FROM {{ ref('fct_mrr')}}
    {{ dbt_utils.group_by(n=3) }}

), dim_date AS (

    SELECT DISTINCT
      date_id,
      first_day_of_month
    FROM {{ ref('dim_date')}}
    WHERE first_day_of_month < CURRENT_DATE

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}

), active_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    WHERE subscription_status NOT IN ('Draft', 'Expired')

), all_subscriptions AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), fct_payloads AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}

), dim_gitlab_releases AS (

    SELECT *
    FROM {{ ref('dim_gitlab_releases')}}

), transformed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['first_day_of_month', 'self_managed_active_subscriptions.subscription_id']) }}        AS month_subscription_id,
      first_day_of_month                                                              AS reporting_month,
      self_managed_active_subscriptions.subscription_id,
      active_subscriptions.subscription_name_slugify,
      active_subscriptions.subscription_start_date,
      active_subscriptions.subscription_end_date,
      MAX(fct_payloads.subscription_id) IS NOT NULL                                   AS has_sent_payloads,
      COUNT(DISTINCT fct_payloads.usage_ping_id)                                      AS monthly_payload_counts,
      COUNT(DISTINCT host_id)                                                         AS monthly_host_counts
    FROM self_managed_active_subscriptions
    INNER JOIN dim_product_detail
      ON self_managed_active_subscriptions.product_details_id = dim_product_detail.dim_product_detail_id
        AND product_delivery_type = 'Self-Managed'
    INNER JOIN dim_date ON self_managed_active_subscriptions.date_id = dim_date.date_id
    LEFT JOIN active_subscriptions ON self_managed_active_subscriptions.subscription_id = active_subscriptions.dim_subscription_id
    LEFT JOIN all_subscriptions ON active_subscriptions.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    LEFT JOIN fct_payloads ON all_subscriptions.subscription_id = fct_payloads.subscription_id AND first_day_of_month = DATE_TRUNC('month', fct_payloads.created_at)
    {{ dbt_utils.group_by(n=6) }}

), latest_versions AS (

    SELECT DISTINCT
      first_day_of_month AS reporting_month,
      self_managed_active_subscriptions.subscription_id,
      active_subscriptions.subscription_name_slugify,
      FIRST_VALUE(major_minor_version) OVER (
        PARTITION BY first_day_of_month, active_subscriptions.subscription_name_slugify
        ORDER BY created_at DESC
      ) AS latest_major_minor_version
    FROM self_managed_active_subscriptions
    INNER JOIN dim_product_detail
      ON self_managed_active_subscriptions.product_details_id = dim_product_detail.dim_product_detail_id
        AND product_delivery_type = 'Self-Managed'
    INNER JOIN dim_date ON self_managed_active_subscriptions.date_id = dim_date.date_id
    INNER JOIN active_subscriptions ON self_managed_active_subscriptions.subscription_id = active_subscriptions.dim_subscription_id
    INNER JOIN all_subscriptions ON active_subscriptions.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    INNER JOIN fct_payloads ON all_subscriptions.subscription_id = fct_payloads.subscription_id AND first_day_of_month = DATE_TRUNC('month', fct_payloads.created_at)

), paid_subscriptions_monthly_usage_ping_optin AS (

    SELECT
      transformed.*,
      latest_versions.latest_major_minor_version
    FROM transformed
    LEFT JOIN latest_versions
      ON transformed.reporting_month = latest_versions.reporting_month
        AND transformed.subscription_name_slugify = latest_versions.subscription_name_slugify

), agg_total_subscriptions AS (

    SELECT
      reporting_month AS agg_month,
      COUNT(DISTINCT subscription_name_slugify) AS total_subscrption_count
    FROM paid_subscriptions_monthly_usage_ping_optin
    {{ dbt_utils.group_by(n=1)}}

), monthly_subscription_optin_counts AS (

    SELECT DISTINCT
      paid_subscriptions_monthly_usage_ping_optin.reporting_month,
      latest_major_minor_version,
      major_version,
      minor_version,
      COUNT(DISTINCT subscription_name_slugify)                         AS major_minor_version_subscriptions,
      major_minor_version_subscriptions /  MAX(total_subscrption_count) AS pct_major_minor_version_subscriptions
    FROM paid_subscriptions_monthly_usage_ping_optin
    INNER JOIN dim_gitlab_releases AS gitlab_releases
      ON paid_subscriptions_monthly_usage_ping_optin.latest_major_minor_version = gitlab_releases.major_minor_version
    LEFT JOIN agg_total_subscriptions AS agg ON paid_subscriptions_monthly_usage_ping_optin.reporting_month = agg.agg_month
    {{ dbt_utils.group_by(n=4) }}

), section_metrics AS (

    SELECT *
    FROM {{ ref('sheetload_usage_ping_metrics_sections') }}
    WHERE is_smau OR is_gmau OR clean_metrics_name = 'monthly_active_users_28_days'

), flattened_usage_data AS (

    SELECT DISTINCT
      f.path                           AS ping_name,
      IFF(edition='CE', edition, 'EE') AS edition,
      SPLIT_PART(ping_name, '.', 1)    AS main_json_name,
      SPLIT_PART(ping_name, '.', -1)   AS feature_name,
      REPLACE(f.path, '.','_')         AS full_ping_name,
      FIRST_VALUE(major_minor_version ) OVER (PARTITION BY full_ping_name ORDER BY
                                 major_version ASC,
      minor_version ASC) AS first_version_with_counter
    FROM {{ ref('version_usage_data') }},
      lateral flatten(input => version_usage_data.raw_usage_data_payload, recursive => True) f



), counter_data AS (

    SELECT DISTINCT
      FIRST_VALUE(major_version) OVER (PARTITION BY group_name
                                         ORDER BY release_date ASC) AS major_version,
      FIRST_VALUE(minor_version) OVER (PARTITION BY group_name
                                         ORDER BY release_date ASC) AS minor_version,
      FIRST_VALUE(DATE_TRUNC('month', release_date)) OVER (PARTITION BY group_name ORDER BY
                                 major_version ASC,
      minor_version ASC) AS release_month,
      stage_name,
      section_name,
      group_name,
      is_smau,
      is_gmau,
      FIRST_VALUE(major_minor_version) OVER (PARTITION BY group_name ORDER BY
                                 major_version ASC,
      minor_version ASC) AS first_version_with_counter,
      edition
    FROM flattened_usage_data
    INNER JOIN section_metrics ON flattened_usage_data.ping_name = section_metrics.metrics_path
    LEFT JOIN dim_gitlab_releases AS gitlab_releases  ON flattened_usage_data.first_version_with_counter = gitlab_releases.major_minor_version
    WHERE release_date < CURRENT_DATE AND (is_smau OR is_gmau)

), date_spine AS (

    SELECT DISTINCT first_day_of_month AS reporting_month
    FROM {{ ref('date_details') }}
    WHERE first_day_of_month < CURRENT_DATE
      AND first_day_of_month >= '2018-01-01'

), date_joined AS (

    SELECT
      date_spine.reporting_month,
      first_version_with_counter,
      edition,
      stage_name,
      section_name,
      group_name,
      is_smau,
      is_gmau,
      SUM(pct_major_minor_version_subscriptions) AS pct_subscriptions_with_counters
    FROM date_spine
    INNER JOIN counter_data
      ON date_spine.reporting_month >= release_month
    LEFT JOIN monthly_subscription_optin_counts
      ON date_spine.reporting_month = monthly_subscription_optin_counts.reporting_month
        AND (counter_data.major_version < monthly_subscription_optin_counts.major_version OR
        (counter_data.major_version = monthly_subscription_optin_counts.major_version AND counter_data.minor_version <= monthly_subscription_optin_counts.minor_version))
    WHERE date_spine.reporting_month < DATE_TRUNC('month', CURRENT_DATE)
    {{ dbt_utils.group_by(n=8) }}

)

SELECT *
FROM date_joined
