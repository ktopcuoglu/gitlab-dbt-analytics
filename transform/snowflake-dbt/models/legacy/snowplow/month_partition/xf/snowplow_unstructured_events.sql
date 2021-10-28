WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}

)

, renamed AS (

    SELECT

      event_id::VARCHAR,
      event_name::VARCHAR,
      IFF(unstruct_event='masked', 'masked', TRY_PARSE_JSON(unstruct_event))::VARIANT
                                     AS unstruct_event,
      IFF(unstruct_event='masked', 'masked', TRY_PARSE_JSON(unstruct_event)['data']['data'])::VARIANT
                                     AS unstruct_event_data,
      v_tracker::VARCHAR,
      dvce_created_tstamp::TIMESTAMP,
      derived_tstamp::TIMESTAMP,
      collector_tstamp::TIMESTAMP,
      user_id::VARCHAR                        AS user_custom_id,
      domain_userid::VARCHAR                  AS user_snowplow_domain_id,
      network_userid::VARCHAR                 AS user_snowplow_crossdomain_id,
      domain_sessionid::VARCHAR               AS session_id,
      domain_sessionidx::INT                  AS session_index,
      (page_urlhost || page_urlpath)::VARCHAR AS page_url,
      page_urlscheme::VARCHAR                 AS page_url_scheme,
      page_urlhost::VARCHAR                   AS page_url_host,
      page_urlpath::VARCHAR                   AS page_url_path,
      page_urlfragment::VARCHAR               AS page_url_fragment,
      mkt_medium::VARCHAR                     AS marketing_medium,
      mkt_source::VARCHAR                     AS marketing_source,
      mkt_term::VARCHAR                       AS marketing_term,
      mkt_content::VARCHAR                    AS marketing_content,
      mkt_campaign::VARCHAR                   AS marketing_campaign,
      app_id::VARCHAR,
      br_family::VARCHAR                      AS browser_name,
      br_name::VARCHAR                        AS browser_major_version,
      br_version::VARCHAR                     AS browser_minor_version,
      os_family::VARCHAR                      AS os,
      os_name::VARCHAR                        AS os_name,
      br_lang::VARCHAR                        AS browser_language,
      os_manufacturer::VARCHAR,
      os_timezone::VARCHAR,
      br_renderengine::VARCHAR                AS browser_engine,
      dvce_type::VARCHAR                      AS device_type,
      dvce_ismobile::BOOLEAN                  AS device_is_mobile,

      --change_form
      cf_formid::VARCHAR,
      cf_elementid::VARCHAR,
      cf_nodename::VARCHAR,
      cf_type::VARCHAR,
      cf_elementclasses::VARCHAR,
      --submit_form
      sf_formid::VARCHAR,
      sf_formclasses::VARCHAR,
      --focus_form
      ff_formid::VARCHAR,
      ff_elementid::VARCHAR,
      ff_nodename::VARCHAR,
      ff_elementtype::VARCHAR,
      ff_elementclasses::VARCHAR,
      --link_click
      lc_elementcontent::VARCHAR,
      lc_elementid::VARCHAR,
      lc_elementclasses::VARCHAR,
      lc_elementtarget::VARCHAR,
      lc_targeturl::VARCHAR

    FROM events
    WHERE event = 'unstruct'

)

SELECT *
FROM renamed
