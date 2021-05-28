
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans_source') }}
  
), prep_product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}
  
  ), renamed AS (

    SELECT

      plan_id AS dim_plan_id,
      created_at,
      updated_at,
      plan_name,
      plan_title

    FROM source

), joined AS (

    SELECT
      renamed.*,
      prep_product_tier.dim_product_tier_id
    FROM renamed
    LEFT JOIN prep_product_tier
      ON prep_product_tier.product_delivery_type = 'SaaS'
      AND renamed.plan_name = LOWER(IFF(prep_product_tier.product_tier_name_short != 'Trial: Ultimate',
                                                                  prep_product_tier.product_tier_historical_short,
                                                                  'ultimate_trial'))
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-30",
    updated_date="2021-05-30"
) }}
