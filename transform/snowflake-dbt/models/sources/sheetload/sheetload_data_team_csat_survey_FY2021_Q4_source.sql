WITH source AS (
  
    SELECT * 
    FROM {{ source('sheetload','data_team_csat_survey_FY2021_Q4') }}

), final AS (
    
    SELECT *			               		
    FROM source
      
) 

SELECT * 
FROM final

