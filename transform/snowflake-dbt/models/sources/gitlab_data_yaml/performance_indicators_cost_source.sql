{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'chief_of_staff_team_pi')
    )
}}

SELECT *
FROM intermediate_stage
