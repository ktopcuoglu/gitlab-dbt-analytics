WITH source_data AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE order_type_stamped IS NOT NULL
      AND NOT is_deleted

), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['order_type_stamped']) }}  AS dim_order_type_id,
      order_type_stamped                                     AS order_type_name,
      CASE 
        WHEN order_type_name = '1. New - First Order'
          THEN '1) New - First Order'
        WHEN order_type_name IN ('2. New - Connected', '3. Growth', '4. Contraction', '5. Churn - Partial', '6. Churn - Final')
          THEN '2) Growth (Growth / New - Connected / Churn / Contraction)'
        WHEN order_type_name IN ('7. PS / Other')
          THEN '3) Consumption / PS / Other'
        ELSE 'Missing order_type_name_mapped'
      END       AS order_type_grouped
    FROM source_data

    UNION ALL
    
    SELECT
      MD5('-1')                                              AS dim_order_type_id,
      'Missing order_type_name'                              AS order_type_name,
      'Missing order_type_grouped'                           AS order_type_grouped

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-12-18",
    updated_date="2021-03-23"
) }}
