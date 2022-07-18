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
    ('gainsight_wave_2_3_metrics','gainsight_wave_2_3_metrics')
]) }}


, fct_ping_instance_metric_with_license  AS (
    SELECT *
    FROM {{ ref('fct_ping_instance_metric') }}
    WHERE license_md5 IS NOT NULL
)

, final AS (

    SELECT
    -- usage ping meta data 
    fct_ping_instance_metric_with_license.dim_ping_instance_id                                 AS dim_ping_instance_id, 
    fct_ping_instance_metric_with_license.dim_instance_id                                      AS uuid, 
    fct_ping_instance_metric_with_license.dim_host_id                                          AS dim_host_id, 
    fct_ping_instance_metric_with_license.license_md5                                          AS license_md5,

    -- Subscription, License and CRM account info
    fct_ping_instance_metric_with_license.dim_subscription_id                                  AS dim_subscription_id,
    fct_ping_instance_metric_with_license.dim_license_id                                       AS dim_license_id,
    fct_ping_instance.dim_crm_account_id                                                       AS dim_crm_account_id,
    fct_ping_instance.dim_parent_crm_account_id                                                AS dim_parent_crm_account_id,
    --map_license_subscription.is_usage_ping_license_in_licenseDot,
  --  map_license_subscription.is_license_mapped_to_subscription                                 AS is_license_mapped_to_subscription,
  --  map_license_subscription.is_license_subscription_id_valid                                  AS is_license_subscription_id_valid,
  --  IFF(map_license_subscription.dim_license_id IS NULL, FALSE, TRUE)                          AS is_usage_ping_license_in_CDot,
    fct_ping_instance_metric_with_license.dim_location_country_id                              AS dim_location_country_id,
  --  fct_ping_instance_with_license_MD5.license_user_count                                      AS license_user_count,
    fct_ping_instance_metric_with_license.metrics_path                                         AS metrics_path,
    CASE WHEN metrics_path IN ('gitlab_shared_runners_enabled','container_registry_enabled','object_store.packages.enabled','instance_auto_devops_enabled',
    'prometheus_enabled','gitlab_shared_runners_enabled','prometheus_metrics_enabled','usage_activity_by_stage.manage.group_saml_enabled','geo_enabled') 
     THEN {{ convert_variant_to_boolean_field("metric_value") }} 
         WHEN metrics_path IN ('gitaly.version')
     THEN metric_value::VARCHAR
    ELSE  {{ null_negative_numbers("metric_value") }} END, ---Can be converted to a nested macro?         
     fct_ping_instance_metric_with_license.metric_value                                        AS metric_value,
    fct_ping_instance_metric_with_license.dim_product_tier_id                                  AS dim_product_tier_id,
    fct_ping_instance_metric_with_license.dim_ping_date_id                                     AS dim_ping_date_id,
    fct_ping_instance_metric_with_license.dim_installation_id                                  AS dim_installation_id,
    fct_ping_instance_metric_with_license.ping_created_at                                      AS ping_created_at,
    fct_ping_instance_metric_with_license.dim_subscription_license_id                          AS dim_subscription_license_id

    FROM fct_ping_instance_metric_with_license
    INNER JOIN gainsight_wave_2_3_metrics
      ON fct_ping_instance_metric_with_license.metrics_path = gainsight_wave_2_3_metrics.metric_name
    LEFT JOIN fct_ping_instance
      ON fct_ping_instance_metric_with_license.dim_ping_instance_id =  fct_ping_instance.dim_ping_instance_id
    where fct_ping_instance_metric_with_license.dim_subscription_id IS NOT NULL


), pivoted AS (

    SELECT
      dim_ping_instance_id,
      uuid,
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
      MAX(ping_created_at) AS ping_created_at,
      {{ dbt_utils.pivot('metrics_path', gainsight_wave_metrics, then_value='metric_value') }}
    FROM final
    {{ dbt_utils.group_by(n=13)}}

  
  {% if is_incremental() %}
                
    AND fct_ping_instance_metric_with_license.ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    
  {% endif %}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2022-07-06",
    updated_date="2022-07-11"
) }}