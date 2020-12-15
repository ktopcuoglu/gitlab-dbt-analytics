{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH source AS (

    SELECT *
    FROM {{ref("comp_band_loc_factor_base")}}

    UNION ALL

    SELECT *
    FROM {{ref("sheetload_comp_band_snapshot_base")}}

  )

SELECT * 
FROM source
