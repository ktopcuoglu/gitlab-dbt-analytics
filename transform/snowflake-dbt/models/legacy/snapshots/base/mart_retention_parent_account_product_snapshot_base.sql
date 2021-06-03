WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'mart_retention_parent_account_product_snapshot') }}
    
)

SELECT *
FROM base
