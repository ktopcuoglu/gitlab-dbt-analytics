{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'finance_team_pi')
    )
}}

SELECT *
FROM intermediate_stage
