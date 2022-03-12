{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, source AS (

    SELECT
      id                                                                        AS dim_usage_ping_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash
    FROM {{ ref('version_usage_data_source') }}

), raw_usage_data AS (

    SELECT *
    FROM {{ ref('version_raw_usage_data_source') }}

), map_ip_to_country AS (

    SELECT *
    FROM {{ ref('map_ip_to_country') }}

), locations AS (

    SELECT *
    FROM {{ ref('prep_location_country') }}

), usage_data AS (

    SELECT
      dim_usage_ping_id                                                                               AS dim_service_ping_id,
      host_id                                                                                         AS dim_host_id,
      uuid                                                                                            AS dim_instance_id,
      ping_created_at,
      source_ip_hash                                                                                  AS ip_address_hash,
      edition                                                                                         AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }}
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%')

), joined_ping AS (

    SELECT
      dim_service_ping_id,
      dim_host_id,
      usage_data.dim_instance_id,
      ping_created_at,
      ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      IFF(original_edition = 'CE', 'CE', 'EE')                                                            AS main_edition,
      CASE
        WHEN original_edition = 'CE'                                     THEN 'Core'
        WHEN original_edition = 'EE Free'                                THEN 'Core'
        WHEN license_expires_at < ping_created_at                        THEN 'Core'
        WHEN original_edition = 'EE'                                     THEN 'Starter'
        WHEN original_edition = 'EES'                                    THEN 'Starter'
        WHEN original_edition = 'EEP'                                    THEN 'Premium'
        WHEN original_edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                                      AS product_tier,
      COALESCE(raw_usage_data.raw_usage_data_payload, usage_data.raw_usage_data_payload_reconstructed)     AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

), map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash,
      map_ip_to_country.dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), add_country_info_to_usage_ping AS (

    SELECT
      joined_ping.*,
      map_ip_location.dim_location_country_id
    FROM joined_ping
    LEFT JOIN map_ip_location
      ON joined_ping.ip_address_hash = map_ip_location.ip_address_hash

), dim_product_tier AS (

  SELECT *
  FROM {{ ref('dim_product_tier') }}
  WHERE product_delivery_type = 'Self-Managed'

), prep_usage_ping_cte AS (

    SELECT
      dim_service_ping_id,
      dim_host_id,
      dim_instance_id,
      dim_product_tier.dim_product_tier_id AS dim_product_tier_id,
      ping_created_at,
      DATEADD('days', -28, ping_created_at)              AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)               AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                 AS ping_created_at_date,
      raw_usage_data_id                                  AS raw_usage_data_id,
      raw_usage_data_payload,
      license_md5,
      dim_location_country_id,
      product_tier,
      main_edition
    FROM add_country_info_to_usage_ping
    LEFT OUTER JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND main_edition = 'EE'

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      prep_license.dim_license_id,
      prep_subscription.dim_subscription_id,
      dim_date.date_id,
      TO_DATE(raw_usage_data.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (raw_usage_data.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)                                       AS is_trial
    FROM prep_usage_ping_cte
    LEFT JOIN raw_usage_data
      ON prep_usage_ping_cte.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    LEFT JOIN prep_license
      ON prep_usage_ping_cte.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(ping_created_at) = dim_date.date_day

), dim_product_tier AS (

    SELECT *
    FROM {{ ref('dim_product_tier') }}

), prep_usage_ping_payload_cte AS (

    SELECT
      joined_payload.dim_service_ping_id,
      dim_product_tier.dim_product_tier_id                   AS dim_product_tier_id,
      COALESCE(license_subscription_id, dim_subscription_id) AS dim_subscription_id,
      dim_license_id,
      dim_location_country_id,
      date_id                                                AS dim_date_id,
      dim_instance_id,
      dim_host_id                                            AS dim_host_id,
      is_trial,
      umau_value,
      license_subscription_id,
      ping_created_at                       AS ping_created_at,
      ping_created_at_date                  AS ping_created_at_date,
      raw_usage_data_payload
    FROM joined_payload
    LEFT JOIN dim_product_tier
      ON TRIM(LOWER(joined_payload.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
      AND main_edition = 'EE'

), flattened_high_level as (
    SELECT
      dim_service_ping_id                   AS dim_service_ping_id,
      path                                  AS metrics_path,
      value                                 AS metric_value,
      dim_product_tier_id                   AS dim_product_tier_id,
      dim_subscription_id                   AS dim_subscription_id,
      dim_location_country_id               AS dim_location_country_id,
      dim_date_id                           AS dim_date_id,
      dim_instance_id                       AS dim_instance_id,
      dim_host_id                           AS dim_host_id,
      dim_license_id                        AS dim_license_id,
      ping_created_at                       AS ping_created_at,
      ping_created_at_date                  AS ping_created_at_date,
      is_trial                              AS is_trial,
      umau_value                            AS umau_value,
      license_subscription_id               AS dim_subscription_license_id,
      'VERSION_DB'                          AS data_source
  FROM prep_usage_ping_payload_cte,
    LATERAL FLATTEN(input => raw_usage_data_payload,
    RECURSIVE => true)

), metric_attributes AS (

    SELECT * FROM dim_usage_ping_metric
        WHERE time_frame != 'none'

), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['dim_service_ping_id', 'flattened_high_level.metrics_path']) }}       AS fct_service_ping_id,
        flattened_high_level.*,
        metric_attributes.is_paid_gmau,
        metric_attributes.time_frame
    FROM flattened_high_level
        INNER JOIN metric_attributes
    ON flattened_high_level.metrics_path = metric_attributes.metrics_path

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-03-08",
    updated_date="2022-03-11"
) }}
