WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'mart_arr_snapshot') }}
    
)

SELECT *
FROM base
