{{config({
    "materialized":"table"
  })
}}

-- depends_on: {{ ref('snowplow_structured_events') }}
WITH unioned_table AS (

{{ schema_union_all('snowplow_', 'snowplow_structured_events', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

)

SELECT *
FROM unioned_table
--filter to the last rolling 24 months of data for query performance tuning
WHERE DATE_TRUNC(MONTH, derived_tstamp::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE)) 