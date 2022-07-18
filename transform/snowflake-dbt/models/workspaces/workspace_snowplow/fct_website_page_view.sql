{{ 
    config(
        materialized='table'
    ) 
}}

{{ 
    simple_cte([
    ('page_views', 'snowplow_page_views_all'),
    ('unstruct_events', 'snowplow_unstructured_events'),
    ('dim_website_page', 'dim_website_page')
    ])

}}

, page_views_w_dim AS (

    SELECT
      REGEXP_REPLACE(page_url_path, '^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', '') AS clean_urlpath,
      dim_website_page_sk,
      session_id,
      page_view_start                                                            AS page_view_start_at,
      page_view_end                                                              AS page_view_end_at,
      time_engaged_in_s                                                          AS engaged_seconds
    FROM page_views
    LEFT JOIN dim_website_page ON page_views.clean_urlpath = dim_website_page.clean_urlpath
)

SELECT *
FROM page_views_w_dim
