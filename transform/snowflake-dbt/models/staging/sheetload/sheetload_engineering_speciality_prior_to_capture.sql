WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_speciality_prior_to_capture_source') }}

)

SELECT *
FROM source
