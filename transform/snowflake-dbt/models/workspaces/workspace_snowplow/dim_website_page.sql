{{ config(
        materialized = "incremental",
        unique_key = "dim_website_page_sk",
) }}

{{ simple_cte([
    ('events', 'snowplow_unnested_events_all')
    ])

}}

, page AS (

    SELECT
      app_id,
      page_urlhost,
      RTRIM(
        REGEXP_REPLACE(page_urlpath, '^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', '')
        , '/')                                                                      AS clean_urlpath,
      SPLIT_PART(clean_urlpath, '/' ,1)                                             AS page_group,
      SPLIT_PART(clean_urlpath, '/' ,2)                                             AS page_type,
      SPLIT_PART(clean_urlpath, '/' ,3)                                             AS page_sub_type,
      refr_medium                                                                   AS referrer_medium,
      max(uploaded_at)                                                              AS max_event_timestamp
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')
    AND page_urlpath IS NOT NULL

    {% if is_incremental() %}

    AND uploaded_at > (SELECT max(max_event_timestamp)) FROM {{ this }})

    {% endif %}

    {{ dbt_utils.group_by(n=7) }}

), dim_with_pk AS (

    SELECT DISTINCT
      --surrogate_key
      {{ dbt_utils.surrogate_key(['page_urlhost','clean_urlpath']) }}               AS dim_website_page_sk,
      app_id,
      page_urlhost,
      clean_urlpath,
      page_group,
      page_type,
      page_sub_type,
      referrer_medium,
      max_event_timestamp
    FROM page

)

SELECT *
FROM dim_with_pk
