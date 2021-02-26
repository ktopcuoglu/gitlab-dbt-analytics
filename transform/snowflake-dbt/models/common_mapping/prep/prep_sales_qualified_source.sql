WITH source_data AS (

    SELECT
      CASE
        WHEN sales_qualified_source = 'BDR Generated' THEN 'SDR Generated'
        ELSE sales_qualified_source
      END                                               AS sales_qualified_source
    FROM {{ref('sfdc_opportunity_source')}}
    WHERE sales_qualified_source IS NOT NULL
      AND NOT is_deleted

), unioned AS (

    SELECT DISTINCT
      MD5(CAST(COALESCE(CAST(sales_qualified_source AS varchar), '') AS varchar))  AS dim_sales_qualified_source_id,
      sales_qualified_source                                                       AS sales_qualified_source_name
    FROM source_data

    UNION ALL

    SELECT
      MD5('-1')                                                                    AS dim_sales_qualified_source_id,
      'Missing opportunity_source_name'                                            AS sales_qualified_source_name

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-10-26",
    updated_date="2021-02-26"
) }}
