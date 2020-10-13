{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'corporate_finance_pi')
    )
}}

SELECT *
FROM intermediate_stage
