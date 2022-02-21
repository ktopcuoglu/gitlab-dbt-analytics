-- depends on: {{ ref('snowplow_gitlab_events') }}

{{ schema_union_limit('snowplow_', 'snowplow_gitlab_events', 'derived_tstamp', 30, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
