{{
    config(
        materialized='incremental',
        unique_key='page_view_id'
    )
}}


WITH all_events AS (

    SELECT * 
    FROM {{ ref('snowplow_base_events') }}

), events AS (

    SELECT * 
    FROM all_events
    {% if is_incremental() %}
        WHERE collector_tstamp > DATEADD('days', -1 * {{ var('snowplow:page_view_lookback_days') }}
         , (SELECT MAX(collector_tstamp) FROM {{this}}))
    {% endif %}

), web_page_context AS (

    SELECT * 
    FROM {{ ref('snowplow_web_page_context') }}

), prep AS (

    SELECT

      ev.event_id,

      ev.user_id,
      ev.domain_userid,
      ev.network_userid,

      ev.collector_tstamp,

      ev.domain_sessionid,
      ev.domain_sessionidx,

      wp.page_view_id,

      ev.page_title,

      ev.page_urlscheme,
      ev.page_urlhost,
      ev.page_urlport,
      ev.page_urlpath,
      ev.page_urlquery,
      ev.page_urlfragment,

      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlport,
      ev.refr_urlpath,
      ev.refr_urlquery,
      ev.refr_urlfragment,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,

      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,

      ev.geo_country,
      ev.geo_region,
      ev.geo_region_name,
      ev.geo_city,
      ev.geo_zipcode,
      ev.geo_latitude,
      ev.geo_longitude,
      ev.geo_timezone,

      ev.user_ipaddress,

      ev.ip_isp,
      ev.ip_organization,
      ev.ip_domain,
      ev.ip_netspeed,

      ev.app_id,

      ev.useragent,
      ev.br_name,
      ev.br_family,
      ev.br_version,
      ev.br_type,
      ev.br_renderengine,
      ev.br_lang,
      ev.dvce_type,
      ev.dvce_ismobile,

      ev.os_name,
      ev.os_family,
      ev.os_manufacturer,
      replace(ev.os_timezone, '%2F', '/') AS os_timezone,

      ev.name_tracker, -- included to filter on
      ev.dvce_created_tstamp -- included to sort on

      {%- for column in var('snowplow:pass_through_columns') %}
        , ev.{{column}}
      {% endfor %}


    FROM events AS ev
        INNER JOIN web_page_context AS wp  
          ON ev.event_id = wp.root_id

    WHERE ev.platform = 'web'
      AND ev.event_name = 'page_view'

)

    SELECT 
      *
    FROM prep
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp))=1
