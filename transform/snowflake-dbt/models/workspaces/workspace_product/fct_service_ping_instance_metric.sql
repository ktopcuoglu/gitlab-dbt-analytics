{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "fct_service_ping_instance_metric_id"
) }}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_service_ping_instance', 'prep_service_ping_instance_flattened'),
    ('dim_service_ping_metric', 'dim_service_ping_metric')
    ])

}}

, map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash                 AS ip_address_hash,
      map_ip_to_country.dim_location_country_id         AS dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_service_ping_instance.*
    FROM prep_service_ping_instance
      {% if is_incremental() %}
                  WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      map_ip_location.dim_location_country_id     AS dim_location_country_id
    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), dim_product_tier AS (

  SELECT *
  FROM {{ ref('dim_product_tier') }}

), prep_usage_ping_cte AS (

    SELECT
      dim_service_ping_instance_id                        AS dim_service_ping_instance_id,
      dim_host_id                                         AS dim_host_id,
      dim_instance_id                                     AS dim_instance_id,
      dim_installation_id                                 AS dim_installation_id,
      dim_product_tier.dim_product_tier_id                AS dim_product_tier_id,
      ping_created_at                                     AS ping_created_at,
      license_md5                                         AS license_md5,
      dim_location_country_id                             AS dim_location_country_id,
      license_trial_ends_on                               AS license_trial_ends_on,
      license_subscription_id                             AS license_subscription_id,
      umau_value                                          AS umau_value,
      product_tier                                        AS product_tier,
      main_edition                                        AS main_edition,
      metrics_path                                        AS metrics_path,
      metric_value                                        AS metric_value,
      has_timed_out                                       AS has_timed_out
    FROM add_country_info_to_usage_ping
    LEFT JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND IFF( add_country_info_to_usage_ping.dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f','SaaS','Self-Managed') = dim_product_tier.product_delivery_type
    --AND main_edition = 'EE'

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      prep_license.dim_license_id                                                                          AS dim_license_id,
      dim_date.date_id                                                                                     AS dim_service_ping_date_id,
      COALESCE(license_subscription_id, prep_subscription.dim_subscription_id)                             AS dim_subscription_id,
      IFF(prep_usage_ping_cte.ping_created_at < license_trial_ends_on, TRUE, FALSE)                        AS is_trial
    FROM prep_usage_ping_cte
    LEFT JOIN prep_license
      ON prep_usage_ping_cte.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

), flattened_high_level as (
    SELECT
      dim_service_ping_instance_id                                                    AS dim_service_ping_instance_id,
      metrics_path                                                                    AS metrics_path,
      metric_value                                                                    AS metric_value,
      has_timed_out                                                                   AS has_timed_out,
      dim_product_tier_id                                                             AS dim_product_tier_id,
      dim_subscription_id                                                             AS dim_subscription_id,
      dim_location_country_id                                                         AS dim_location_country_id,
      dim_service_ping_date_id                                                        AS dim_service_ping_date_id,
      dim_instance_id                                                                 AS dim_instance_id,
      dim_host_id                                                                     AS dim_host_id,
      dim_installation_id                                                             AS dim_installation_id,
      dim_license_id                                                                  AS dim_license_id,
      ping_created_at                                                                 AS ping_created_at,
      umau_value                                                                      AS umau_value,
      license_subscription_id                                                         AS dim_subscription_license_id,
      'VERSION_DB'                                                                    AS data_source
  FROM joined_payload

), metric_attributes AS (

    SELECT * FROM dim_service_ping_metric
      -- WHERE time_frame != 'none'

), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['dim_service_ping_instance_id', 'flattened_high_level.metrics_path']) }}       AS fct_service_ping_instance_metric_id,
        flattened_high_level.*,
        metric_attributes.time_frame                                                                               AS time_frame
    FROM flattened_high_level
        LEFT JOIN metric_attributes
    ON flattened_high_level.metrics_path = metric_attributes.metrics_path

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-03-08",
    updated_date="2022-03-11"
) }}
