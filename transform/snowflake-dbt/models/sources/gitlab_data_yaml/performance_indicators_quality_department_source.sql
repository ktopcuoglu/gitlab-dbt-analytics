{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'quality_department_pi')
    )
}}

SELECT *
FROM intermediate_stage
