{{ config(
    tags=["product"]
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
    ('prep_usage_ping', 'prep_usage_ping')
    ])
    
}}

, joined AS (

    SELECT 
      {{ dbt_utils.star(from=ref('prep_usage_ping'), relation_alias='prep_usage_ping', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      main_edition                                                         AS edition,
      main_edition_product_tier                                                             AS edition_product_tier,
      ping_source                                                          AS usage_ping_delivery_type, 
      prep_license.dim_license_id,
      prep_subscription.dim_subscription_id,
      dim_date.date_id,
      TO_DATE(raw_usage_data.raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (raw_usage_data.raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      raw_usage_data.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)                                       AS is_trial

    FROM prep_usage_ping
    LEFT JOIN raw_usage_data
      ON prep_usage_ping.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    LEFT JOIN prep_license
      ON prep_usage_ping.license_md5 = prep_license.license_md5
    LEFT JOIN prep_subscription
      ON prep_license.dim_subscription_id = prep_subscription.dim_subscription_id
    LEFT JOIN dim_date 
      ON TO_DATE(ping_created_at) = dim_date.date_day
      
), dim_product_tier AS (
  
    SELECT * 
    FROM {{ ref('dim_product_tier') }}
    WHERE product_delivery_type = 'Self-Managed'

), final AS (

    SELECT 
      joined.dim_usage_ping_id,
      dim_product_tier.dim_product_tier_id               AS dim_product_tier_id,
      dim_subscription_id,
      dim_license_id,
      dim_location_country_id,
      date_id                                            AS dim_date_id,
      dim_instance_id,

      -- timestamps
      ping_created_at,
      DATEADD('days', -28, ping_created_at)              AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)               AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                 AS ping_created_at_date,
      raw_usage_data_id                                  AS raw_usage_data_id, 
      
      -- metadata
      raw_usage_data_payload, 
      edition, 
      product_tier, 
      edition_product_tier, 
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      usage_ping_delivery_type,
      is_internal, 
      is_staging, 
      is_trial,
      instance_user_count,
      host_name
    FROM joined
    LEFT JOIN dim_product_tier
      ON TRIM(LOWER(joined.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
      AND edition = 'EE'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2021-05-10",
    updated_date="2021-05-10"
) }}
