WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_data_team_csat_survey_fy2021_q4_source') }}

)

SELECT *
FROM source

