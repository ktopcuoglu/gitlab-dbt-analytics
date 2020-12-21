WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','data_team_csat_survey_FY2021_Q4') }}

), final AS (
    
  SELECT 
    NULLIF(suvery_timestamp, '')::VARCHAR::TIMESTAMP                               AS suvery_timestamp,
    NULLIF(division, '')::VARCHAR	                                               AS divsion,
    NULLIF(team_member_role, '')::VARCHAR                                          AS team_member_role,
    NULLIF(location, '')::VARCHAR                                                  AS location,
    NULLIF(interaction_with_data_team, '')::VARCHAR                                AS interaction_with_data_team,
    NULLIF(how_often_interact_with_data_team, '')::VARCHAR                         AS how_often_interact_with_data_team,
    NULLIF(data_team_solutions, '')::VARCHAR                                       AS data_team_solutions,
    TRY_TO_NUMBER(data_team_importance_rate) 	                                   AS data_team_importance_rate,
    TRY_TO_NUMBER(data_team_collaboration_rate)	                                   AS data_team_collaboration_rate,
    TRY_TO_NUMBER(data_team_results_rate) 	                                       AS data_team_results_rate,
    TRY_TO_NUMBER(data_team_efficiency_rate)	                                   AS data_team_efficiency_rate,
    TRY_TO_NUMBER(data_team_diversity_rate)	                                       AS data_team_diversity_rate,
    TRY_TO_NUMBER(data_team_iteration_rate)	                                       AS data_team_iteration_rate,
    TRY_TO_NUMBER(data_team_transparency_rate)	                                   AS data_team_transparency_rate,
    TRY_TO_NUMBER(data_team_overall_quality_rate)		                           AS data_team_overall_quality_rate,
    NULLIF(what_data_team_is_doing_well, '')::VARCHAR	                           AS what_data_team_is_doing_well,
    NULLIF(what_data_team_needs_to_improve, '')::VARCHAR						   AS what_data_team_needs_to_improve			
FROM source

) 

SELECT * 
FROM final

