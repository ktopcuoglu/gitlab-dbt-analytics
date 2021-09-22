WITH tiers AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}

), license AS (

    SELECT *
    FROM {{ ref('prep_license') }}

), final AS (

    SELECT
      dim_license_id,

      dim_subscription_id,
      dim_subscription_id_original,
      dim_subscription_id_previous,
      tiers.dim_product_tier_id,

      license_md5,
      subscription_name,
      environment,
      license_user_count,
      license_plan,
      is_trial,
      is_internal,
      company,
      license_start_date,
      license_expire_date,
      created_at,
      updated_at
    FROM license
    LEFT JOIN tiers
      ON LOWER(tiers.product_tier_name) = license.license_plan 
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@jpeguero",
    created_date="2021-01-08",
    updated_date="2021-09-22"
) }}
