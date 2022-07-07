{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

{{ simple_cte([
    ('instance_types', 'dim_host_instance_type'),
    ('map_license_subscription','map_license_subscription_account'),
    ('fct_ping_instance','fct_ping_instance')
    ('prep_ping_instance','prep_ping_instance')
]) }}

, prep_ping_instance_with_license AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_subscription_mapped') }}
    WHERE license_md5 IS NOT NULL

), final AS (

    SELECT

    {{ default_usage_ping_information() }}

    instance_types.instance_type,
    -- Subscription, License and CRM account info
    fct_ping_instance.dim_subscription_id,
    fct_ping_instance.dim_license_id,
    fct_ping_instance.dim_crm_account_id,
    fct_ping_instance.dim_parent_crm_account_id,
    map_license_subscription.is_usage_ping_license_in_licenseDot,
    map_license_subscription.is_license_mapped_to_subscription,
    map_license_subscription.is_license_subscription_id_valid,
    fct_ping_instance.dim_location_country_id,
    prep_ping_instance_with_license.license_user_count,

    {{ sales_wave_2_3_metrics() }}

    FROM prep_ping_instance_with_license
    LEFT JOIN instance_types
      ON prep_ping_instance_with_license.raw_usage_data_payload['uuid']::VARCHAR = instance_types.instance_uuid
      AND prep_ping_instance_with_license.raw_usage_data_payload['hostname']::VARCHAR = instance_types.instance_hostname
    LEFT JOIN map_license_subscription_account
      ON usage_pings_with_license_md5.license_md5 = REPLACE(map_license_subscription_account.license_md5, '-')
    LEFT OUTER JOIN fct_ping_instance 
      ON prep_ping_instance_with_license.dim_ping_instance_id = fct_ping_instance.dim_ping_instance_id
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_usage_ping_id
        ORDER BY ping_created_at DESC
      ) = 1
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-06",
    updated_date="2022-07-06"
) }}