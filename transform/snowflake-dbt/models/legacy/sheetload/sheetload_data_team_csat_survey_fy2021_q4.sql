WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_data_team_csat_survey_fy2021_q4_source') }}

), final AS (

    SELECT 
        NULLIF("Timestamp", '')::VARCHAR::TIMESTAMP                                                                                                 AS survey_timestamp,
        NULLIF("Which_division_are_you_a_part_of?", '')::VARCHAR                                                                                    AS divsion,
        NULLIF("What_is_your_role_at_GitLab?", '')::VARCHAR                                                                                         AS role,
        NULLIF("Where_are_you_located?", '')::VARCHAR                                                                                               AS location,
        NULLIF("How_do_you_normally_interact_with_the_Data_Team?", '')::VARCHAR                                                                     AS interaction_with_data_team,
        NULLIF("How_often_do_you_interact_with_the_Data_Team?", '')::VARCHAR                                                                        AS how_often_interaction_with_data_team,
        NULLIF("What_Data_Team_solutions_do_you_regularly_use?_Please_check_all_that_apply.", '')::VARCHAR                                          AS data_team_solutions,
        TRY_TO_NUMBER("How_important_is_the_Data_Team_to_your_success?_")                                                                           AS data_team_importance_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Collaboration.")                                               AS data_team_collaboration_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Results._")                                                    AS data_team_results_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Efficiency.")                                                  AS data_team_efficiency_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Diversity,_Inclusion,_&_Belonging.")                           AS data_team_diversity_inclusion_belonging_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Iteration.")                                                   AS data_team_iteration_rating,
        TRY_TO_NUMBER("Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Transparency.")                                                AS data_team_transparency_rating,
        TRY_TO_NUMBER("How_would_you_rate_the_Overall_Quality_of_the_Results_you_received_from_the_Data_Team?")                                     AS data_team_overall_quality_rating,
        NULLIF("What_is_the_Data_Team_doing_well?_Please_be_specific_as_possible.", '')::VARCHAR                                                    AS what_data_team_is_doing_well,
        NULLIF("What_can_the_Data_Team_improve_on?_Please_be_specific_as_possible.", '')::VARCHAR                                                   AS what_data_team_needs_to_improve
    FROM source

)

SELECT *
FROM final
