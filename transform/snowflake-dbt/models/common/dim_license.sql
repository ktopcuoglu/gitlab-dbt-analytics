WITH license AS (

     SELECT
       dim_license_id,
       license_md5,
       dim_subscription_id,
       subscription_name,
       dim_subscription_id_original,
       dim_subscription_id_previous,
       license_user_count,
       license_plan,
       dim_product_tier_id,
       is_trial,
       is_internal,
       company,
       license_start_date,
       license_expire_date,
       created_at,
       updated_at
    FROM {{ ref('prep_license') }}

)


{{ dbt_audit(
    cte_ref="license",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-01-08",
    updated_date="2021-01-08"
) }}
