{{ config(
    tags=["product", "mnpi_exception"],
    full_refresh = false,
    materialized = "incremental",
    unique_key = "prep_service_ping_instance_flattened_id"
) }}


WITH source AS (

    SELECT
        *
    FROM {{ ref('prep_service_ping_instance')}} as usage
    {% if is_incremental() %}
          WHERE ping_created_at >= (SELECT COALESCE(MAX(ping_created_at) FROM {{this}}), '2022-01-01')
    {% endif %}

) , flattened_high_level as (
      SELECT
        {{ dbt_utils.surrogate_key(['dim_service_ping_instance_id', 'path']) }}       AS prep_service_ping_instance_flattened_id,
        dim_service_ping_instance_id,
        dim_host_id,
        dim_instance_id,
        ping_created_at,
        ip_address_hash,
        license_md5,
        main_edition,
        product_tier,
        TO_DATE(source.raw_usage_data_payload:license_trial_ends_on::TEXT)                     AS license_trial_ends_on,
        (source.raw_usage_data_payload:license_subscription_id::TEXT)                          AS license_subscription_id,
        source.raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER    AS umau_value,
        path                                  AS metrics_path,
        value                                 AS metric_value
      FROM source,
        LATERAL FLATTEN(input => raw_usage_data_payload,
        RECURSIVE => true)

  )

  {{ dbt_audit(
      cte_ref="flattened_high_level",
      created_by="@icooper-acp",
      updated_by="@icooper-acp",
      created_date="2022-03-17",
      updated_date="2022-03-17"
  ) }}
