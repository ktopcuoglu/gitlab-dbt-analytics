{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
  "materialized": "incremental",
  "unique_key": "fct_service_ping_instance_id"
    })
}}

{%- set settings_columns = dbt_utils.get_column_values(table=ref('prep_usage_ping_metrics_setting'), column='metrics_path', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_license', 'prep_license'),
    ('prep_subscription', 'prep_subscription'),
    ('prep_usage_ping_metrics_setting', 'prep_usage_ping_metrics_setting'),
    ('dim_date', 'dim_date'),
    ('map_ip_to_country', 'map_ip_to_country'),
    ('locations', 'prep_location_country'),
    ('prep_service_ping_instance', 'prep_service_ping_instance'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, map_ip_location AS (

    SELECT
      map_ip_to_country.ip_address_hash,
      map_ip_to_country.dim_location_country_id
    FROM map_ip_to_country
    INNER JOIN locations
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), source AS (

    SELECT
      prep_service_ping_instance.*
    FROM prep_service_ping_instance
      {% if is_incremental() %}
                  WHERE
                      ping_created_at > (SELECT COALESCE(DATEADD(WEEK,-2,MAX(ping_created_at)),dateadd(month, -24, current_date()))            -- Check on First Time UUID/Instance_IDs returns '2000-01-01'
                                  FROM {{this}} AS usage_ping
                                  WHERE prep_service_ping_instance.dim_instance_id  = usage_ping.dim_instance_id)
      {% endif %}

), add_country_info_to_usage_ping AS (

    SELECT
      source.*,
      map_ip_location.dim_location_country_id
    FROM source
    LEFT JOIN map_ip_location
      ON source.ip_address_hash = map_ip_location.ip_address_hash

), dim_product_tier AS (

  SELECT *
  FROM {{ ref('dim_product_tier') }}

), prep_usage_ping_cte AS (

    SELECT
      dim_service_ping_instance_id,
      dim_host_id,
      dim_instance_id,
      dim_product_tier.dim_product_tier_id               AS dim_product_tier_id,
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
      main_edition,
      metrics_path,
      metric_value
    FROM add_country_info_to_usage_ping
    LEFT OUTER JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND IFF( add_country_info_to_usage_ping.dim_instance_id = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f','SaaS','Self-Managed') = dim_product_tier.product_delivery_type
    AND main_edition = 'EE'

), joined_payload AS (

    SELECT
      prep_usage_ping_cte.*,
      prep_license.dim_license_id,
      dim_date.date_id                                                                                            AS dim_date_id,
      TO_DATE(prep_service_ping_instance.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (prep_service_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      COALESCE(license_subscription_id, prep_subscription.dim_subscription_id)                                    AS dim_subscription_id,
      prep_service_ping_instance.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      IFF(prep_usage_ping_cte.ping_created_at < license_trial_ends_on, TRUE, FALSE)                               AS is_trial
    FROM prep_usage_ping_cte
    LEFT JOIN prep_service_ping_instance
      ON prep_usage_ping_cte.raw_usage_data_id = prep_service_ping_instance.raw_usage_data_id
    LEFT JOIN prep_license
      ON prep_usage_ping_cte.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date
      ON TO_DATE(prep_usage_ping_cte.ping_created_at) = dim_date.date_day

), flattened_high_level as (
    SELECT
      dim_service_ping_instance_id          AS dim_service_ping_instance_id,
      metrics_path                          AS metrics_path,
      metric_value                          AS metric_value,
      dim_product_tier_id                   AS dim_product_tier_id,
      dim_subscription_id                   AS dim_subscription_id,
      dim_location_country_id               AS dim_location_country_id,
      dim_date_id                           AS dim_date_id,
      dim_instance_id                       AS dim_instance_id,
      dim_host_id                           AS dim_host_id,
      dim_host_id || dim_instance_id        AS dim_installation_id,
      dim_license_id                        AS dim_license_id,
      ping_created_at                       AS ping_created_at,
      ping_created_at_date                  AS ping_created_at_date,
      umau_value                            AS umau_value,
      license_subscription_id               AS dim_subscription_license_id,
      'VERSION_DB'                          AS data_source
  FROM joined_payload

), metric_attributes AS (

    SELECT * FROM dim_usage_ping_metric
      -- WHERE time_frame != 'none'

), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['dim_service_ping_instance_id', 'flattened_high_level.metrics_path']) }}       AS fct_service_ping_instance_id,
        flattened_high_level.*,
        metric_attributes.time_frame
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
