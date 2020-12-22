{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_sessions') }}

{{ schema_union_all('snowplow_', 'snowplow_structured_events', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
