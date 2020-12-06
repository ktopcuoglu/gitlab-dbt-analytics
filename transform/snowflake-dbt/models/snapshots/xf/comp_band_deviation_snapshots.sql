{{ config({
    "schema": "temporary",
    })
}}

WITH source AS (

    SELECT *
    FROM {{ref("comp_band_loc_factor_base")}}

    --capturing prior to 2020.05.20

    UNION ALL

    SELECT *
    FROM {{ref("sheetload_comp_band_snapshot_base")}}
    --capturing after 2020.10.30

)

SELECT *
FROM source