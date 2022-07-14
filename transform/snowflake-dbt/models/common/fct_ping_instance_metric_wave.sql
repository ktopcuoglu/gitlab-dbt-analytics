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
    ('fct_ping_instance','fct_ping_instance')
]) }}


, dim_ping_instance_with_license_MD5  AS (
    SELECT *
    FROM {{ ref('dim_ping_instance') }}
    WHERE license_md5 IS NOT NULL
)

, final AS (

    SELECT
    -- usage ping meta data 
    dim_ping_instance_with_license_MD5.dim_ping_instance_id                                    AS dim_ping_instance_id, 
    dim_ping_instance_with_license_MD5.ping_created_at                                         AS ping_created_at,
    dim_ping_instance_with_license_MD5.ping_created_date_28_days_earlier                       AS ping_created_date_28_days_earlier,
    dim_ping_instance_with_license_MD5.ping_created_date_year                                  AS ping_created_date_year,
    dim_ping_instance_with_license_MD5.ping_created_date_month                                 AS ping_created_date_month,
    dim_ping_instance_with_license_MD5.ping_created_date_week                                  AS ping_created_date_week,
    dim_ping_instance_with_license_MD5.ping_created_at_date                                    AS ping_created_at_date,
 
    -- instance settings 
    fct_ping_instance_with_license_MD5.uuid                                                    AS uuid, 
    dim_ping_instance.ping_delivery_type                                                       AS ping_delivery_type, 
    dim_ping_instance.version                                                                  AS instance_version, 
    dim_ping_instance.cleaned_version                                                          AS cleaned_version,
    dim_ping_instance.version_is_prerelease                                                    AS version_is_prerelease,
    dim_ping_instance.major_version                                                            AS major_version,
    dim_ping_instance.minor_version                                                            AS minor_version,
    dim_ping_instance.major_minor_version                                                      AS major_minor_version,
    fct_ping_instance_with_license_MD5.product_tier                                            AS product_tier, 
    fct_ping_instance_with_license_MD5.main_edition_product_tier                               AS main_edition_product_tier,
    dim_ping_instance.host_name                                                                AS hostname, 
    dim_ping_instance.dim_host_id                                                              AS host_id, 
    dim_ping_instance.installation_type                                                        AS installation_type, 
    dim_ping_instance.is_internal                                                              AS is_internal, 
    dim_ping_instance.is_staging                                                               AS is_staging, 
   -- instance user statistics 
    fct_ping_instance_with_license_MD5.license_billable_users                                  AS license_billable_users, 
    fct_ping_instance_with_license_MD5.instance_user_count                                     AS instance_user_count, 
    fct_ping_instance_with_license_MD5.historical_max_users                                    AS historical_max_users, 
    fct_ping_instance_with_license_MD5.license_md5                                             AS license_md5,
    instance_types.instance_type                                                               AS instance_type,

    -- Subscription, License and CRM account info
    fct_ping_instance_with_license_MD5.dim_subscription_id                                     AS dim_subscription_id,
    fct_ping_instance_with_license_MD5.dim_license_id                                          AS dim_license_id,
    fct_ping_instance_with_license_MD5.dim_crm_account_id                                      AS dim_crm_account_id,
    fct_ping_instance_with_license_MD5.dim_parent_crm_account_id                               AS dim_parent_crm_account_id,
    --map_license_subscription.is_usage_ping_license_in_licenseDot,
    map_license_subscription.is_license_mapped_to_subscription                                 AS is_license_mapped_to_subscription,
    map_license_subscription.is_license_subscription_id_valid                                  AS is_license_subscription_id_valid,
    IFF(map_license_subscription.dim_license_id IS NULL, FALSE, TRUE)                          AS is_usage_ping_license_in_CDot,
    fct_ping_instance_with_license_MD5.dim_location_country_id                                 AS dim_location_country_id,
    fct_ping_instance_with_license_MD5.license_user_count                                      AS license_user_count,

    {{ sales_wave_2_3_metrics() }}

    FROM fct_ping_instance_with_license_MD5
    LEFT JOIN dim_ping_instance
      ON fct_ping_instance_with_license_MD5.dim_ping_instance_id =  dim_ping_instance.dim_ping_instance_id
    LEFT JOIN instance_types
      ON fct_ping_instance_with_license_MD5.uuid = instance_types.instance_uuid
      AND dim_ping_instance.host_name = instance_types.instance_hostname
    LEFT JOIN map_license_subscription
      ON fct_ping_instance_with_license_MD5.license_md5 = REPLACE(map_license_subscription.license_md5, '-')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY fct_ping_instance_with_license_MD5.dim_ping_instance_id
        ORDER BY fct_ping_instance_with_license_MD5.ping_created_at DESC
      ) = 1

  {% if is_incremental() %}
                
    AND fct_ping_instance_with_license_MD5.ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    
  {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-06",
    updated_date="2022-07-11"
) }}