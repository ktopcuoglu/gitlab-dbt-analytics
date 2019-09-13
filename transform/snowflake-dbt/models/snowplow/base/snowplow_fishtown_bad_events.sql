{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "materialized":"table",
    "unique_key":"base64_event",
    "schema":current_date_schema('snowplow')
  })
}}

WITH base as (

    SELECT *
    FROM {{ source('fishtown_snowplow', 'bad_events') }}
    WHERE length(JSONTEXT['errors']) > 0
      AND date_part(month, uploaded_at::timestamp) = '{{ month_value }}'
      AND date_part(year, uploaded_at::timestamp) = '{{ year_value }}'

), renamed AS (

    SELECT DISTINCT JSONTEXT [ 'line' ] :: string              AS base64_event,
                    TO_ARRAY(JSONTEXT [ 'errors' ])            AS error_array,
                    JSONTEXT [ 'failure_tstamp' ] :: timestamp AS failure_timestamp,
                    'Fishtown'                                 AS infra_source,
                    uploaded_at
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp