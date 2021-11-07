WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}

)

, renamed AS (

    SELECT

      event_id::VARCHAR                      AS event_id,
      event_name::VARCHAR                    AS event_name,
      IFF(unstruct_event='masked', 'masked', TRY_PARSE_JSON(unstruct_event))::VARIANT
                                     AS unstruct_event,
      IFF(unstruct_event='masked', 'masked', TRY_PARSE_JSON(unstruct_event)['data']['data'])::VARIANT
                                     AS unstruct_event_data,
      v_tracker::VARCHAR                      AS v_tracker,
      dvce_created_tstamp::TIMESTAMP          AS dvce_created_tstamp,
      derived_tstamp::TIMESTAMP               AS derived_tstamp,
      collector_tstamp::TIMESTAMP             AS collector_tstamp,
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
      app_id::VARCHAR                         AS app_id,
      br_family::VARCHAR                      AS browser_name,
      br_name::VARCHAR                        AS browser_major_version,
      br_version::VARCHAR                     AS browser_minor_version,
      os_family::VARCHAR                      AS os,
      os_name::VARCHAR                        AS os_name,
      br_lang::VARCHAR                        AS browser_language,
      os_manufacturer::VARCHAR                AS os_manufacturer,
      os_timezone::VARCHAR                    AS os_timezone,
      br_renderengine::VARCHAR                AS browser_engine,
      dvce_type::VARCHAR                      AS device_type,
      dvce_ismobile::BOOLEAN                  AS device_is_mobile,

      -- standard context
      gsc_environment                         AS gsc_environment,
      gsc_extra                               AS gsc_extra,
      gsc_namespace_id                        AS gsc_namespace_id,
      gsc_plan                                AS gsc_plan,
      gsc_google_analytics_client_id          AS gsc_google_analytics_client_id,
      gsc_project_id                          AS gsc_project_id,
      gsc_pseudonymized_user_id               AS gsc_pseudonymized_user_id,

      --change_form
      cf_formid::VARCHAR                      AS cf_formid,
      cf_elementid::VARCHAR                   AS cf_elementid,
      cf_nodename::VARCHAR                    AS cf_nodename,
      cf_type::VARCHAR                        AS cf_type,
      cf_elementclasses::VARCHAR              AS cf_elementclasses,
      --submit_form
      sf_formid::VARCHAR                      AS sf_formid,
      sf_formclasses::VARCHAR                 AS sf_formclasses,
      --focus_form
      ff_formid::VARCHAR                      AS ff_formid,
      ff_elementid::VARCHAR                   AS ff_elementid,
      ff_nodename::VARCHAR                    AS ff_nodename,
      ff_elementtype::VARCHAR                 AS ff_elementtype,
      ff_elementclasses::VARCHAR              AS ff_elementclasses,
      --link_click
      lc_elementcontent::VARCHAR              AS lc_elementcontent,
      lc_elementid::VARCHAR                   AS lc_elementid,
      lc_elementclasses::VARCHAR              AS lc_elementclasses,
      lc_elementtarget::VARCHAR               AS lc_elementtarget,
      lc_targeturl::VARCHAR                   AS lc_targeturl

    FROM events
    WHERE event = 'unstruct'

)

SELECT *
FROM renamed
