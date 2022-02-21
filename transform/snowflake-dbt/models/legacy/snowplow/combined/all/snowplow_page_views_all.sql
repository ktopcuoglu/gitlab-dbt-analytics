{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_page_views') }}

{{ schema_union_all('snowplow_', 'snowplow_page_views', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}
