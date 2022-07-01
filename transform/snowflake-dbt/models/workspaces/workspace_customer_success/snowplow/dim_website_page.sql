{{ 
    config(
        materialized='table'
    ) 
}}

{{ 
    simple_cte([
    ('events', 'snowplow_unnested_events_all')
    ])

}}

, page AS (

    SELECT DISTINCT
      app_id,
      page_urlhost,
      REGEXP_REPLACE(page_urlpath, '[^a-zA-Z]', '')                       AS letters_urlpath,
      REGEXP_REPLACE(split_part(page_urlpath, '/' ,2), '[^a-zA-Z]', '')   AS page_group,
      REGEXP_REPLACE(split_part(page_urlpath, '/' ,3), '[^a-zA-Z]', '')   AS page_type,
      REGEXP_REPLACE(split_part(page_urlpath, '/' ,5), '[^a-zA-Z]', '')   AS page_sub_type,
      refr_medium                                                         AS referrer_medium
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')

), dim_with_pk AS (

    SELECT DISTINCT
      --surrogate_key
      {{ dbt_utils.surrogate_key(['page.page_group', 'page.page_type', 'page.page_sub_type']) }} AS dim_website_page_sk,
      letters_urlpath,
      app_id,
      page_urlhost,
      page_group,
      page_type,
      page_sub_type,
      referrer_medium
    FROM page

)

SELECT *
FROM dim_with_pk