{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

{{ simple_cte([
    ('instance_types', 'dim_host_instance_type')
]) }}

, prep_usage_ping AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_subscription_mapped') }}
    WHERE license_md5 IS NOT NULL

), final AS (

    SELECT

    {{ default_usage_ping_information() }}

    instance_types.instance_type,
    -- subscription_info
    is_usage_ping_license_in_licenseDot,
    dim_license_id,
    dim_subscription_id,
    is_license_mapped_to_subscription,
    is_license_subscription_id_valid,
    prep_usage_ping.dim_crm_account_id,
    dim_parent_crm_account_id,

    {{ sales_wave_2_3_metrics() }}

    FROM prep_usage_ping
    LEFT JOIN instance_types
      ON prep_usage_ping.raw_usage_data_payload['uuid']::VARCHAR = instance_types.instance_uuid
      AND prep_usage_ping.raw_usage_data_payload['hostname']::VARCHAR = instance_types.instance_hostname

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@ischweickartDD",
    created_date="2021-01-10",
    updated_date="2021-04-05"
) }}