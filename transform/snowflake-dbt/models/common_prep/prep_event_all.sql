{{ config(
    tags=["product"],
    materialized = "view"
) }}

-- depends on: {{ ref('prep_event') }}
WITH unioned_table AS (

{{ schema_union_all('dotcom_usage_events_', 'prep_event', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

)

SELECT *
FROM unioned_table
-- Some past events may change in the source system and they need to be filtered out of
-- the static month partitions
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_created_at DESC) = 1
