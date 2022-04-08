WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hp_dvr_source') }}

)

SELECT *
FROM source