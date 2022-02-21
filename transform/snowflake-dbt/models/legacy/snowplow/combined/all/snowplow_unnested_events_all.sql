{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_unnested_events') }}

{{ schema_union_all('snowplow_', 'snowplow_unnested_events', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
