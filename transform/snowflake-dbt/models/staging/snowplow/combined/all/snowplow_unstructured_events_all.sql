{{config({
    "materialized":"view"
  })
}}

{{ schema_union_all('snowplow', 'snowplow_unstructured_events') }}
