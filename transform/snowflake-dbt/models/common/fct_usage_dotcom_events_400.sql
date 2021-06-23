{{ schema_union_limit('dotcom_usage_events_', 'prep_dotcom_usage_events', 'event_created_at', 400, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
