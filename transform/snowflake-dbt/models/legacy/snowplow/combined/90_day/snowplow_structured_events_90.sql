{{config({
    "materialized":"table"
  })
}}

-- depends_on: {{ ref('snowplow_structured_events') }}

{{ schema_union_limit('snowplow_', 'snowplow_structured_events', 'derived_tstamp', 90, database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
