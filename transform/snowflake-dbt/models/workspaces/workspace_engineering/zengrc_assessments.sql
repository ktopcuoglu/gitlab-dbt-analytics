WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_assessment_source') }}

)

SELECT *
FROM source