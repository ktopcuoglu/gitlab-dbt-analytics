WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'fct_retention_snapshot') }}
    
)

SELECT *
FROM base
