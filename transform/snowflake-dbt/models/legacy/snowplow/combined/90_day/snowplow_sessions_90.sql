-- depends on: {{ ref('snowplow_sessions') }}

{{ schema_union_limit('snowplow_', 'snowplow_sessions', 'session_start', 90, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
