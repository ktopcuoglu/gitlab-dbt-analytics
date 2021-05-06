WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sdr_attainment_to_goal_source') }}

)

SELECT *
FROM source
