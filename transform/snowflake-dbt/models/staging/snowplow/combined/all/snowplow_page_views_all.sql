{{config({
    "materialized":"view"
  })
}}

{{ schema_union_all('snowplow', 'snowplow_page_views') }}
