WITH source_data AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE order_type_stamped IS NOT NULL
      AND NOT is_deleted

), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['order_type_stamped']) }}  AS dim_order_type_id,
      order_type_stamped                                     AS order_type_name,
      order_type_grouped
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
