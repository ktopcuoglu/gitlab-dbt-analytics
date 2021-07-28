WITH source_data AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_source')}}
    WHERE sales_qualified_source IS NOT NULL
      AND NOT is_deleted

), unioned AS (

    SELECT DISTINCT
      MD5(CAST(COALESCE(CAST(sales_qualified_source AS varchar), '') AS varchar))  AS dim_sales_qualified_source_id,
      sales_qualified_source                                                       AS sales_qualified_source_name,
      sales_qualified_source_grouped                                               AS sales_qualified_source_grouped
    FROM source_data

    UNION ALL

    SELECT
      MD5('-1')                                                                    AS dim_sales_qualified_source_id,
      'Missing sales_qualified_source_name'                                        AS sales_qualified_source_name,
      'Web Direct Generated'                                                       AS sales_qualified_source_grouped

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-10-26",
    updated_date="2021-04-26"
) }}
