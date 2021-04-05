{{ datasiren.search_sample_columns_samples_for_pattern(this.identifier, env_var('SNOWFLAKE_LOAD_DATABASE'), 'TEXT', '100', '25', '^(.*\\D)*((\\d{3}-\\d{2}-\\d{4})|(\\d{9}))(\\D.*)*$') }}
