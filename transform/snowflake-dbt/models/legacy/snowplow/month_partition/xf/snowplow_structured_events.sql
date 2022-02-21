WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}

)

, renamed AS (
  
    SELECT
    
      event_id::VARCHAR                         AS event_id,
      v_tracker::VARCHAR                        AS v_tracker,
      se_action::VARCHAR                        AS event_action,
      se_category::VARCHAR                      AS event_category,
      se_label::VARCHAR                         AS event_label,
      se_property::VARCHAR                      AS event_property,
      se_value::VARCHAR                          AS event_value,
      TRY_PARSE_JSON(contexts)::VARIANT         AS contexts,
      dvce_created_tstamp::TIMESTAMP            AS dvce_created_tstamp,
      derived_tstamp::TIMESTAMP                 AS derived_tstamp,
      collector_tstamp::TIMESTAMP               AS collector_tstamp,
      user_id::VARCHAR                          AS user_custom_id,
      domain_userid::VARCHAR                    AS user_snowplow_domain_id,
      network_userid::VARCHAR                   AS user_snowplow_crossdomain_id,
      domain_sessionid::VARCHAR                 AS session_id,
      domain_sessionidx::INT                    AS session_index,
      (page_urlhost || page_urlpath)::VARCHAR   AS page_url,
      page_urlscheme::VARCHAR                   AS page_url_scheme,
      page_urlhost::VARCHAR                     AS page_url_host,
      page_urlpath::VARCHAR                     AS page_url_path,
      page_urlfragment::VARCHAR                 AS page_url_fragment,
      mkt_medium::VARCHAR                       AS marketing_medium,
      mkt_source::VARCHAR                       AS marketing_source,
      mkt_term::VARCHAR                         AS marketing_term,
      mkt_content::VARCHAR                      AS marketing_content,
      mkt_campaign::VARCHAR                     AS marketing_campaign,
      app_id::VARCHAR                           AS app_id,
      br_family::VARCHAR                        AS browser_name,
      br_name::VARCHAR                          AS browser_major_version,
      br_version::VARCHAR                       AS browser_minor_version,
      os_family::VARCHAR                        AS os,
      os_name::VARCHAR                          AS os_name,
      br_lang::VARCHAR                          AS browser_language,
      os_manufacturer::VARCHAR                  AS os_manufacturer,
      os_timezone::VARCHAR                      AS os_timezone,
      br_renderengine::VARCHAR                  AS browser_engine,
      dvce_type::VARCHAR                        AS device_type,
      dvce_ismobile::BOOLEAN                    AS device_is_mobile,
      gsc_environment                           AS gsc_environment,
      gsc_extra                                 AS gsc_extra,
      gsc_namespace_id                          AS gsc_namespace_id,
      gsc_plan                                  AS gsc_plan,
      gsc_google_analytics_client_id            AS gsc_google_analytics_client_id,
      gsc_project_id                            AS gsc_project_id,
      gsc_pseudonymized_user_id                 AS gsc_pseudonymized_user_id,
      gsc_source                                AS gsc_source

    FROM events
    WHERE event = 'struct'

)

SELECT *
FROM renamed
