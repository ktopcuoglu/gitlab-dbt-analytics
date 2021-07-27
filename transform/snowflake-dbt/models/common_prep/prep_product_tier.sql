WITH product_tier_mapping AS (

    SELECT *
    FROM {{ ref('map_product_tier') }}

), mapping AS (

    SELECT DISTINCT 
      product_tier_historical,
      product_tier,
      product_delivery_type,
      product_ranking        
    FROM product_tier_mapping
    
    UNION ALL
    
    SELECT
      'SaaS - Free'                                                 AS product_tier_historical,
      'SaaS - Free'                                                 AS product_tier,
      'SaaS'                                                        AS product_delivery_type,
      0                                                             AS product_ranking
    
    UNION ALL
    
    SELECT
      'Self-Managed - Core'                                         AS product_tier_historical,
      'Self-Managed - Free'                                         AS product_tier,
      'Self-Managed'                                                AS product_delivery_type,
      0                                                             AS product_ranking
  
    UNION ALL
    
    SELECT
      'SaaS - Trial: Gold'                                          AS product_tier_historical,
      'SaaS - Trial: Ultimate'                                      AS product_tier,
      'SaaS'                                                        AS product_delivery_type,
      0                                                             AS product_ranking
  
    UNION ALL
    
    SELECT
      'Self-Managed - Trial: Ultimate'                              AS product_tier_historical,
      'Self-Managed - Trial: Ultimate'                              AS product_tier,
      'Self-Managed'                                                AS product_delivery_type,
      0                                                             AS product_ranking

), final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['product_tier_historical']) }}      AS dim_product_tier_id,
    product_tier_historical,
    SPLIT_PART(product_tier_historical, ' - ', -1)                  AS product_tier_historical_short,
    product_tier                                                    AS product_tier_name,
    SPLIT_PART(product_tier, ' - ', -1)                             AS product_tier_name_short,
    product_delivery_type,
    product_ranking
  FROM mapping
  
  UNION ALL
  
  SELECT
    MD5('-1')                                                       AS dim_product_tier_id,
    '(Unknown Historical Tier)'                                     AS product_tier_historical,
    '(Unknown Historical Tier Name)'                                AS product_tier_historical_short,
    '(Unknown Tier)'                                                AS product_tier_name,
    '(Unknown Tier Name)'                                           AS product_tier_name_short,
    '(Unknown Delivery Type)'                                       AS product_delivery_type,
    -1                                                              AS product_ranking

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@ischweickartDD",
    created_date="2020-12-29",
    updated_date="2021-01-26"
) }}
