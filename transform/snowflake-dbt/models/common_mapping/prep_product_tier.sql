{{config({
    "schema": "common_mapping"
  })
}}

WITH product_tier_mapping AS (

    SELECT *
    FROM {{ ref('map_product_tier') }}

), mapping AS (

    SELECT DISTINCT 
      product_tier,
      product_delivery_type,
      product_ranking        
    FROM product_tier_mapping
    
    UNION
    
    SELECT
      'Free'                                                                 AS product_tier,
      'SaaS'                                                                 AS product_delivery_type,
      0                                                                      AS product_ranking
    
    UNION
    
    SELECT
      'Core'                                                                 AS product_tier,
      'Self-Managed'                                                         AS product_delivery_type,
      0                                                                      AS product_ranking
  
    UNION
    
    SELECT
      'Trial'                                                                AS product_tier,
      'SaaS'                                                                 AS product_delivery_type,
      0                                                                      AS product_ranking

), final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['product_tier']) }}                          AS dim_product_tier_id,
    product_tier                                                             AS product_tier_name,
    product_delivery_type,
    product_ranking
  FROM mapping
  
  UNION ALL
  
  SELECT
    MD5('-1')                                                                AS dim_product_tier_id,
    '(Unknown Tier)'                                                         AS product_tier_name,
    '(Unknown Delivery Type)'                                                AS product_delivery_type,
    -1                                                                       AS product_ranking

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2020-12-29",
    updated_date="2020-12-29"
) }}