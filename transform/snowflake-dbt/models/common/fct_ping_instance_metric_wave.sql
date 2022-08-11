{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_ping_instance_id"
    })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('gainsight_wave_2_3_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('fct_ping_instance', 'fct_ping_instance'),
    ('gainsight_wave_2_3_metrics','gainsight_wave_2_3_metrics'),
    ('dim_ping_instance','dim_ping_instance')

]) }}


, fct_ping_instance_metric_with_license  AS (
    SELECT *
    FROM {{ ref('fct_ping_instance_metric') }}
    WHERE license_md5 IS NOT NULL
)

, final AS (

    SELECT
  
    fct_ping_instance_metric_with_license.dim_ping_instance_id                                 AS dim_ping_instance_id, 
    fct_ping_instance_metric_with_license.dim_instance_id                                      AS dim_instance_id, 
    fct_ping_instance_metric_with_license.dim_host_id                                          AS dim_host_id, 
    fct_ping_instance_metric_with_license.license_md5                                          AS license_md5,
    fct_ping_instance_metric_with_license.dim_subscription_id                                  AS dim_subscription_id,
    fct_ping_instance_metric_with_license.dim_license_id                                       AS dim_license_id,
    fct_ping_instance.dim_crm_account_id                                                       AS dim_crm_account_id,
    fct_ping_instance.dim_parent_crm_account_id                                                AS dim_parent_crm_account_id,
    fct_ping_instance_metric_with_license.dim_location_country_id                              AS dim_location_country_id,
    fct_ping_instance_metric_with_license.dim_product_tier_id                                  AS dim_product_tier_id, 
    fct_ping_instance_metric_with_license.dim_ping_date_id                                     AS dim_ping_date_id,
    fct_ping_instance_metric_with_license.dim_installation_id                                  AS dim_installation_id,
    fct_ping_instance_metric_with_license.dim_subscription_license_id                          AS dim_subscription_license_id,
    fct_ping_instance.license_user_count                                                       AS license_user_count,
    fct_ping_instance.license_billable_users                                                   AS license_billable_users,
    fct_ping_instance.historical_max_user_count                                                AS historical_max_user_count,
    fct_ping_instance.instance_user_count                                                      AS instance_user_count,
    fct_ping_instance_metric_with_license.metrics_path                                         AS metrics_path,
    fct_ping_instance_metric_with_license.metric_value                                         AS metric_value,    
    fct_ping_instance_metric_with_license.ping_created_at                                      AS ping_created_at,
    dim_ping_instance.ping_created_date_month                                                  AS ping_created_date_month,
    fct_ping_instance.hostname                                                                 AS hostname,
    fct_ping_instance_metric_with_license.is_license_mapped_to_subscription                    AS is_license_mapped_to_subscription,
    fct_ping_instance_metric_with_license.is_license_mapped_to_subscription                    AS is_license_subscription_id_valid,
    fct_ping_instance_metric_with_license.is_service_ping_license_in_customerDot               AS is_service_ping_license_in_customerDot,
    dim_ping_instance.ping_delivery_type                                                       AS ping_delivery_type,
    dim_ping_instance.cleaned_version                                                          AS cleaned_version

    FROM fct_ping_instance_metric_with_license
    INNER JOIN gainsight_wave_2_3_metrics
      ON fct_ping_instance_metric_with_license.metrics_path = gainsight_wave_2_3_metrics.metric_name
    LEFT JOIN fct_ping_instance
      ON fct_ping_instance_metric_with_license.dim_ping_instance_id =  fct_ping_instance.dim_ping_instance_id
    LEFT JOIN dim_ping_instance
      ON fct_ping_instance_metric_with_license.dim_ping_instance_id =  dim_ping_instance.dim_ping_instance_id
     WHERE fct_ping_instance_metric_with_license.dim_subscription_id IS NOT NULL

), pivoted AS (

    SELECT
      dim_ping_instance_id,
      dim_instance_id,
      ping_created_at,
      ping_created_date_month,
      ping_delivery_type,
      dim_host_id,
      dim_subscription_id,
      dim_license_id,
      license_md5,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_location_country_id,
      dim_product_tier_id,
      dim_ping_date_id,
      dim_installation_id,
      dim_subscription_license_id,
      license_user_count,
      license_billable_users,
      historical_max_user_count,
      instance_user_count,
      hostname,
      cleaned_version,
      is_license_mapped_to_subscription,
      is_license_subscription_id_valid,
      is_service_ping_license_in_customerDot,
      {{ ping_instance_wave_metrics() }}

    FROM final
        QUALIFY ROW_NUMBER() OVER (
      PARTITION BY final.dim_ping_instance_id
        ORDER BY final.ping_created_at DESC
      ) = 1

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-06",
    updated_date="2022-07-21"
) }}