WITH source_data AS (

    SELECT *
    FROM {{ ref('prep_sfdc_account') }}
    WHERE dim_account_sales_segment_name_source IS NOT NULL
    
), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.surrogate_key(['dim_account_sales_segment_name_source']) }}  AS dim_sales_segment_id,
      dim_account_sales_segment_name_source                                     AS sales_segment_name,
      dim_account_sales_segment_grouped_source                                  AS sales_segment_grouped
    FROM source_data
    
    UNION ALL

    SELECT
      MD5('-1')                                                                 AS dim_sales_segment_id,
      'Missing sales_segment_name'                                              AS sales_segment_name,
      'Missing sales_segment_grouped'                                           AS sales_segment_grouped

)



{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-12-18",
    updated_date="2021-04-26"
) }}
