WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_mapping_sdr_sfdc_bamboohr_source') }}

)

SELECT *
FROM source
