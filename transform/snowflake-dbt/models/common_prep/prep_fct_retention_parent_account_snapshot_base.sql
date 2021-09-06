WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'fct_retention_parent_account_snapshot') }}
    
)

SELECT *
FROM base
