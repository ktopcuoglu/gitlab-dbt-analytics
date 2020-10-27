{{ config({
    "schema": "temporary"
    })
}}

WITH SOURCE as (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_comp_band_snapshots') }}

)

SELECT *
FROM source
