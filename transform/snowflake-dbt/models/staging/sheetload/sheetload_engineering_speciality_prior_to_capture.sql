WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_speciality_prior_to_capture') }}

)

SELECT *
FROM source
