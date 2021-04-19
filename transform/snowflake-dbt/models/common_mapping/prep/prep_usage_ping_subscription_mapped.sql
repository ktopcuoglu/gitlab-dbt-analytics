{{
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

WITH usage_pings_with_license_md5 AS (

    SELECT *
    FROM {{ ref('prep_usage_ping') }}
    WHERE license_md5 IS NOT NULL

), map_license_subscription_account AS (

    SELECT *
    FROM  {{ ref('map_license_subscription_account') }}

), final AS (

    SELECT
      usage_pings_with_license_md5.dim_usage_ping_id,
      usage_pings_with_license_md5.dim_product_tier_id,
      usage_pings_with_license_md5.ping_created_at,
      usage_pings_with_license_md5.ping_created_at_28_days_earlier,
      usage_pings_with_license_md5.ping_created_at_year,
      usage_pings_with_license_md5.ping_created_at_month,
      usage_pings_with_license_md5.ping_created_at_week,
      usage_pings_with_license_md5.ping_created_at_date,
      usage_pings_with_license_md5.raw_usage_data_id,
      usage_pings_with_license_md5.raw_usage_data_payload,
      usage_pings_with_license_md5.license_md5,
      usage_pings_with_license_md5.original_edition,
      usage_pings_with_license_md5.edition,
      usage_pings_with_license_md5.main_edition,
      usage_pings_with_license_md5.product_tier,
      usage_pings_with_license_md5.main_edition_product_tier,
      usage_pings_with_license_md5.cleaned_version,
      usage_pings_with_license_md5.version_is_prerelease,
      usage_pings_with_license_md5.major_version,
      usage_pings_with_license_md5.minor_version,
      usage_pings_with_license_md5.major_minor_version,
      usage_pings_with_license_md5.ping_source,
      usage_pings_with_license_md5.is_internal,
      usage_pings_with_license_md5.is_staging,
      usage_pings_with_license_md5.country_name,
      usage_pings_with_license_md5.iso_2_country_code,
      usage_pings_with_license_md5.iso_3_country_code,
      map_license_subscription_account.dim_license_id,
      map_license_subscription_account.dim_subscription_id,
      map_license_subscription_account.is_license_mapped_to_subscription,
      map_license_subscription_account.is_license_subscription_id_valid,
      map_license_subscription_account.dim_crm_account_id,
      map_license_subscription_account.dim_parent_crm_account_id,
      IFF(map_license_subscription_account.dim_license_id IS NULL, FALSE, TRUE)   AS is_usage_ping_license_in_licenseDot
    FROM usage_pings_with_license_md5
    LEFT JOIN map_license_subscription_account
      ON usage_pings_with_license_md5.license_md5 = map_license_subscription_account.license_md5

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@ischweickartDD",
    created_date="2021-01-10",
    updated_date="2021-04-05"
) }}