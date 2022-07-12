{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_ping_instance_id"
    })
}}

{{ simple_cte([
    ('instance_types', 'dim_host_instance_type'),
    ('map_license_subscription','map_license_subscription_account'),
    ('fct_ping_instance','fct_ping_instance'),
    ('dim_ping_instance','dim_ping_instance')
]) }}

, final AS (

    SELECT
    -- usage ping meta data 
    fct_ping_instance.dim_ping_instance_id              AS dim_ping_instance_id, 
    dim_ping_instance.ping_created_at                    AS ping_created_at,
    dim_ping_instance.ping_created_at_28_days_earlier    AS ping_created_at_28_days_earlier,
    dim_ping_instance.ping_created_at_year               AS ping_created_at_year,
    dim_ping_instance.ping_created_at_month              AS ping_created_at_month, 
    dim_ping_instance.ping_created_at_week               AS ping_created_at_week,
    dim_ping_instance.ping_created_at_date               AS ping_created_at_date,
 
    -- instance settings 
    fct_ping_instance.uuid                               AS uuid, 
    --ping_source, 
    dim_ping_instance.version                            AS instance_version, 
    dim_ping_instance.cleaned_version                    AS cleaned_version,
    dim_ping_instance.version_is_prerelease              AS version_is_prerelease,
    dim_ping_instance.major_version                      AS major_version,
    dim_ping_instance.minor_version                      AS minor_version,
    dim_ping_instance.major_minor_version                AS major_minor_version,
    fct_ping_instance.product_tier                       AS product_tier, 
    fct_ping_instance.main_edition_product_tier          AS main_edition_product_tier,
    dim_ping_instance.host_name                          AS hostname, 
    dim_ping_instance.dim_host_id                        AS host_id, 
    dim_ping_instance.installation_type                  AS installation_type, 
    dim_ping_instance.is_internal                        AS is_internal, 
    dim_ping_instance.is_staging                         AS is_staging, 
   -- instance user statistics 
    fct_ping_instance.license_billable_users             AS license_billable_users, 
    fct_ping_instance.instance_user_count                AS instance_user_count, 
    fct_ping_instance.historical_max_users               AS historical_max_users, 
    fct_ping_instance.license_md5                        AS license_md5,
    instance_types.instance_type,
    instance_types.instance_type,

    -- Subscription, License and CRM account info
    fct_ping_instance.dim_subscription_id,
    fct_ping_instance.dim_license_id,
    fct_ping_instance.dim_crm_account_id,
    fct_ping_instance.dim_parent_crm_account_id,
    --map_license_subscription.is_usage_ping_license_in_licenseDot,
    map_license_subscription.is_license_mapped_to_subscription,
    map_license_subscription.is_license_subscription_id_valid,
    fct_ping_instance.dim_location_country_id,
    fct_ping_instance.license_user_count,

    {{ sales_wave_2_3_metrics() }}

    FROM fct_ping_instance
    LEFT JOIN dim_ping_instance
      ON fct_ping_instance.dim_ping_instance_id =  dim_ping_instance.dim_ping_instance_id
    LEFT JOIN instance_types
      ON fct_ping_instance.uuid = instance_types.instance_uuid
      AND dim_ping_instance.host_name = instance_types.instance_hostname
    LEFT JOIN map_license_subscription
      ON fct_ping_instance.license_md5 = REPLACE(map_license_subscription.license_md5, '-')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_ping_instance_id
        ORDER BY ping_created_at DESC
      ) = 1

  {% if is_incremental() %}
                
    AND fct_ping_instance.ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    
  {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-06",
    updated_date="2022-07-11"
) }}