
{{ schema_union_limit('dotcom_usage_events_', 'prep_event', 'event_created_at', 800, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
