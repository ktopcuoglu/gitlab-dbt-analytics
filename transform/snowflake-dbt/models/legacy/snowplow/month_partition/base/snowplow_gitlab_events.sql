{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"event_id"
  })
}}

{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


WITH filtered_source as (

    SELECT
      app_id,
      base_currency,
      br_colordepth,
      br_cookies,
      br_family,
      br_features_director,
      br_features_flash,
      br_features_gears,
      br_features_java,
      br_features_pdf,
      br_features_quicktime,
      br_features_realplayer,
      br_features_silverlight,
      br_features_windowsmedia,
      br_lang,
      br_name,
      br_renderengine,
      br_type,
      br_version,
      br_viewheight,
      br_viewwidth,
      collector_tstamp,
      contexts,
      derived_contexts,
      -- correctting bugs on ruby tracker which was sending wrong timestamp
      -- https://gitlab.com/gitlab-data/analytics/issues/3097
      IFF(DATE_PART('year', TRY_TO_TIMESTAMP(derived_tstamp)) > 1970, 
            derived_tstamp, collector_tstamp) AS derived_tstamp,
      doc_charset,
      try_to_numeric(doc_height)              AS doc_height,
      try_to_numeric(doc_width)               AS doc_width,
      domain_sessionid,
      domain_sessionidx,
      domain_userid,
      dvce_created_tstamp,
      dvce_ismobile,
      dvce_screenheight,
      dvce_screenwidth,
      dvce_sent_tstamp,
      dvce_type,
      etl_tags,
      etl_tstamp,
      event,
      event_fingerprint,
      event_format,
      event_id,
      event_name,
      event_vendor,
      event_version,
      geo_city,
      geo_country,
      geo_latitude,
      geo_longitude,
      geo_region,
      geo_region_name,
      geo_timezone,
      geo_zipcode,
      ip_domain,
      ip_isp,
      ip_netspeed,
      ip_organization,
      mkt_campaign,
      mkt_clickid,
      mkt_content,
      mkt_medium,
      mkt_network,
      mkt_source,
      mkt_term,
      name_tracker,
      network_userid,
      os_family,
      os_manufacturer,
      os_name,
      os_timezone,
      page_referrer,
      page_title,
      page_url,
      page_urlfragment,
      page_urlhost,
      page_urlpath,
      page_urlport,
      page_urlquery,
      page_urlscheme,
      platform,
      try_to_numeric(pp_xoffset_max)          AS pp_xoffset_max,
      try_to_numeric(pp_xoffset_min)          AS pp_xoffset_min,
      try_to_numeric(pp_yoffset_max)          AS pp_yoffset_max,
      try_to_numeric(pp_yoffset_min)          AS pp_yoffset_min,
      refr_domain_userid,
      refr_dvce_tstamp,
      refr_medium,
      refr_source,
      refr_term,
      refr_urlfragment,
      refr_urlhost,
      refr_urlpath,
      refr_urlport,
      refr_urlquery,
      refr_urlscheme,
      se_action,
      se_category,
      se_label,
      se_property,
      se_value,
      ti_category,
      ti_currency,
      ti_name,
      ti_orderid,
      ti_price,
      ti_price_base,
      ti_quantity,
      ti_sku,
      tr_affiliation,
      tr_city,
      tr_country,
      tr_currency,
      tr_orderid,
      tr_shipping,
      tr_shipping_base,
      tr_state,
      tr_tax,
      tr_tax_base,
      tr_total,
      tr_total_base,
      true_tstamp,
      txn_id,
      CASE
        WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
          THEN 'masked'
        ELSE unstruct_event
      END AS unstruct_event,
      user_fingerprint,
      user_id,
      user_ipaddress,
      useragent,
      v_collector,
      v_etl,
      v_tracker,
      uploaded_at,
      'GitLab' AS infra_source
    {% if target.name not in ("prod") -%}

    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

    {%- else %}

    FROM {{ ref('snowplow_gitlab_good_events_source') }}

    {%- endif %}

    WHERE app_id IS NOT NULL
      AND DATE_PART(month, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ month_value }}'
      AND DATE_PART(year, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ year_value }}'
      AND 
        (
          (
            -- js backend tracker
            v_tracker LIKE 'js%'
            AND lower(page_url) NOT LIKE 'https://staging.gitlab.com/%'
            AND lower(page_url) NOT LIKE 'https://customers.stg.gitlab.com/%'
            AND lower(page_url) NOT LIKE 'http://localhost:%'
          )
          
          OR
          
          (
            -- ruby backend tracker
            v_tracker LIKE 'rb%'
          )
        )
      AND TRY_TO_TIMESTAMP(derived_tstamp) is not null
)

, base AS (
  
    SELECT DISTINCT * 
    FROM filtered_source

), events_with_context_flattened AS (
    /*
    we need to extract the web_page_id from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The context we are looking for containing the web_page_id is this one:
      {
      'data': {
      'id': 'de5069f7-32cf-4ad4-98e4-dafe05667089'
      },
      'schema': 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
      }
    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data (where the web_page_id will be contained)
    */
    SELECT 
      base.*,
      f.value['schema']::TEXT     AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

), events_with_web_page_id AS (
    /*
    in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
    the web_page context, which has this context schema: iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0
    Then we extract the id from the context_data column
    */
    SELECT 
      events_with_context_flattened.*,
      context_data['id']::TEXT AS web_page_id
    FROM events_with_context_flattened
    WHERE context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'

), base_with_sorted_columns AS (
  
    SELECT 
      events_with_web_page_id.app_id,
      events_with_web_page_id.base_currency,
      events_with_web_page_id.br_colordepth,
      events_with_web_page_id.br_cookies,
      events_with_web_page_id.br_family,
      events_with_web_page_id.br_features_director,
      events_with_web_page_id.br_features_flash,
      events_with_web_page_id.br_features_gears,
      events_with_web_page_id.br_features_java,
      events_with_web_page_id.br_features_pdf,
      events_with_web_page_id.br_features_quicktime,
      events_with_web_page_id.br_features_realplayer,
      events_with_web_page_id.br_features_silverlight,
      events_with_web_page_id.br_features_windowsmedia,
      events_with_web_page_id.br_lang,
      events_with_web_page_id.br_name,
      events_with_web_page_id.br_renderengine,
      events_with_web_page_id.br_type,
      events_with_web_page_id.br_version,
      events_with_web_page_id.br_viewheight,
      events_with_web_page_id.br_viewwidth,
      events_with_web_page_id.collector_tstamp,
      events_with_web_page_id.contexts,
      events_with_web_page_id.derived_contexts,
      events_with_web_page_id.derived_tstamp,
      events_with_web_page_id.doc_charset,
      events_with_web_page_id.doc_height,
      events_with_web_page_id.doc_width,
      events_with_web_page_id.domain_sessionid,
      events_with_web_page_id.domain_sessionidx,
      events_with_web_page_id.domain_userid,
      events_with_web_page_id.dvce_created_tstamp,
      events_with_web_page_id.dvce_ismobile,
      events_with_web_page_id.dvce_screenheight,
      events_with_web_page_id.dvce_screenwidth,
      events_with_web_page_id.dvce_sent_tstamp,
      events_with_web_page_id.dvce_type,
      events_with_web_page_id.etl_tags,
      events_with_web_page_id.etl_tstamp,
      events_with_web_page_id.event,
      events_with_web_page_id.event_fingerprint,
      events_with_web_page_id.event_format,
      events_with_web_page_id.event_id,
      events_with_web_page_id.web_page_id,
      events_with_web_page_id.event_name,
      events_with_web_page_id.event_vendor,
      events_with_web_page_id.event_version,
      events_with_web_page_id.geo_city,
      events_with_web_page_id.geo_country,
      events_with_web_page_id.geo_latitude,
      events_with_web_page_id.geo_longitude,
      events_with_web_page_id.geo_region,
      events_with_web_page_id.geo_region_name,
      events_with_web_page_id.geo_timezone,
      events_with_web_page_id.geo_zipcode,
      events_with_web_page_id.ip_domain,
      events_with_web_page_id.ip_isp,
      events_with_web_page_id.ip_netspeed,
      events_with_web_page_id.ip_organization,
      events_with_web_page_id.mkt_campaign,
      events_with_web_page_id.mkt_clickid,
      events_with_web_page_id.mkt_content,
      events_with_web_page_id.mkt_medium,
      events_with_web_page_id.mkt_network,
      events_with_web_page_id.mkt_source,
      events_with_web_page_id.mkt_term,
      events_with_web_page_id.name_tracker,
      events_with_web_page_id.network_userid,
      events_with_web_page_id.os_family,
      events_with_web_page_id.os_manufacturer,
      events_with_web_page_id.os_name,
      events_with_web_page_id.os_timezone,
      events_with_web_page_id.page_referrer,
      events_with_web_page_id.page_title,
      events_with_web_page_id.page_url,
      events_with_web_page_id.page_urlfragment,
      events_with_web_page_id.page_urlhost,
      events_with_web_page_id.page_urlpath,
      events_with_web_page_id.page_urlport,
      events_with_web_page_id.page_urlquery,
      events_with_web_page_id.page_urlscheme,
      events_with_web_page_id.platform,
      events_with_web_page_id.pp_xoffset_max,
      events_with_web_page_id.pp_xoffset_min,
      events_with_web_page_id.pp_yoffset_max,
      events_with_web_page_id.pp_yoffset_min,
      events_with_web_page_id.refr_domain_userid,
      events_with_web_page_id.refr_dvce_tstamp,
      events_with_web_page_id.refr_medium,
      events_with_web_page_id.refr_source,
      events_with_web_page_id.refr_term,
      events_with_web_page_id.refr_urlfragment,
      events_with_web_page_id.refr_urlhost,
      events_with_web_page_id.refr_urlpath,
      events_with_web_page_id.refr_urlport,
      events_with_web_page_id.refr_urlquery,
      events_with_web_page_id.refr_urlscheme,
      events_with_web_page_id.se_action,
      events_with_web_page_id.se_category,
      events_with_web_page_id.se_label,
      events_with_web_page_id.se_property,
      events_with_web_page_id.se_value,
      events_with_web_page_id.ti_category,
      events_with_web_page_id.ti_currency,
      events_with_web_page_id.ti_name,
      events_with_web_page_id.ti_orderid,
      events_with_web_page_id.ti_price,
      events_with_web_page_id.ti_price_base,
      events_with_web_page_id.ti_quantity,
      events_with_web_page_id.ti_sku,
      events_with_web_page_id.tr_affiliation,
      events_with_web_page_id.tr_city,
      events_with_web_page_id.tr_country,
      events_with_web_page_id.tr_currency,
      events_with_web_page_id.tr_orderid,
      events_with_web_page_id.tr_shipping,
      events_with_web_page_id.tr_shipping_base,
      events_with_web_page_id.tr_state,
      events_with_web_page_id.tr_tax,
      events_with_web_page_id.tr_tax_base,
      events_with_web_page_id.tr_total,
      events_with_web_page_id.tr_total_base,
      events_with_web_page_id.true_tstamp,
      events_with_web_page_id.txn_id,
      events_with_web_page_id.unstruct_event,
      events_with_web_page_id.user_fingerprint,
      events_with_web_page_id.user_id,
      events_with_web_page_id.user_ipaddress,
      events_with_web_page_id.useragent,
      events_with_web_page_id.v_collector,
      events_with_web_page_id.v_etl,
      events_with_web_page_id.v_tracker,
      events_with_web_page_id.uploaded_at,
      events_with_web_page_id.infra_source
    FROM events_with_web_page_id

), events_to_ignore as (

    SELECT event_id
    FROM base_with_sorted_columns
    GROUP BY 1
    HAVING count (*) > 1

), unnested_unstruct as (

    SELECT *,
    {{dbt_utils.get_url_parameter(field='page_urlquery', url_parameter='glm_source')}} AS glm_source,
    CASE
      WHEN LENGTH(unstruct_event) > 0 AND TRY_PARSE_JSON(unstruct_event) IS NULL
        THEN TRUE
      ELSE FALSE END AS is_bad_unstruct_event,
    {{ unpack_unstructured_event(change_form, 'change_form', 'cf') }},
    {{ unpack_unstructured_event(submit_form, 'submit_form', 'sf') }},
    {{ unpack_unstructured_event(focus_form, 'focus_form', 'ff') }},
    {{ unpack_unstructured_event(link_click, 'link_click', 'lc') }},
    {{ unpack_unstructured_event(track_timing, 'track_timing', 'tt') }}
    FROM base_with_sorted_columns

)


SELECT *
FROM unnested_unstruct
WHERE event_id NOT IN (SELECT * FROM events_to_ignore)
ORDER BY derived_tstamp
