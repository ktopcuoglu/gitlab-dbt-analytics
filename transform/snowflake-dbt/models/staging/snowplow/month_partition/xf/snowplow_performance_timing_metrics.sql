WITH events AS (

    SELECT *
    FROM {{ ref('snowplow_structured_events') }}

), contexts AS (  

    SELECT
      context.value['data'] AS performance_timing,
      event_id,
      derived_tstamp
    FROM events,
    LATERAL FLATTEN(INPUT => contexts, PATH => 'data') AS context
    WHERE context.value['schema'] = 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0'

), parsed_timing AS (

    SELECT
      performance_timing['connectEnd']                  AS connect_end,
      performance_timing['connectStart']                AS connect_start,
      performance_timing['domComplete']                 AS dom_complete,
      performance_timing['domContentLoadedEventEnd']    AS dom_content_loaded_event_end,
      performance_timing['domContentLoadedEventStart']  AS dom_content_loaded_event_start,
      performance_timing['domInteractive']              AS dom_interactive,
      performance_timing['domLoading']                  AS dom_loading,
      performance_timing['domainLookupEnd']             AS domain_lookup_end,
      performance_timing['domainLookupStart']           AS domain_lookup_start,
      performance_timing['fetchStart']                  AS fetch_start,
      performance_timing['loadEventEnd']                AS load_event_end,
      performance_timing['loadEventStart']              AS load_event_start,
      performance_timing['navigationStart']             AS navigation_start,
      performance_timing['redirectEnd']                 AS redirect_end,
      performance_timing['redirectStart']               AS redirect_start,
      performance_timing['requestStart']                AS request_start,
      performance_timing['responseEnd']                 AS response_end,
      performance_timing['responseStart']               AS response_start,
      performance_timing['secureConnectionStart']       AS secure_connection_start,
      performance_timing['unloadEventEnd']              AS unload_event_end,
      performance_timing['unloadEventStart']            AS unload_event_start,
      event_id                                          AS root_id,
      derived_tstamp                                    AS root_tstamp
    FROM contexts
)

SELECT *
FROM parsed_timing