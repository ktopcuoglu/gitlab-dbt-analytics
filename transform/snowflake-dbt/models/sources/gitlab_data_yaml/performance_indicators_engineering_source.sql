{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'engineering_function_pi')
    )
}}

SELECT *
FROM intermediate_stage
