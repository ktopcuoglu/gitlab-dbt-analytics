{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('prep_event') }}

{{ schema_union_all('dotcom_usage_events_', 'prep_event', 'event_created_at', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
