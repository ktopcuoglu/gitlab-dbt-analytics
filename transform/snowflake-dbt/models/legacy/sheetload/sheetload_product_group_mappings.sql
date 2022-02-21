WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_product_group_mappings_source') }}

)

SELECT *
FROM source