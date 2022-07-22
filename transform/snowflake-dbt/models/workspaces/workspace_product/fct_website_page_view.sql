{{ config(
        materialized = "incremental",
        unique_key = "fct_website_page_sk",
) }}

{{ 
    simple_cte([
    ('page_views', 'snowplow_page_views_all'),
    ('unstruct_events', 'snowplow_unstructured_events_all'),
    ('dim_website_page', 'dim_website_page')
    ])

}}

, page_views_w_clean_url AS (

    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS namespace_nk,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS project_nk,
      session_id,
      user_snowplow_domain_id,
      page_view_id                                                                  AS event_id,
      'page_view'                                                                   AS event_name,
      page_view_start                                                               AS page_view_start_at,
      page_view_end                                                                 AS page_view_end_at,
      time_engaged_in_s                                                             AS engaged_seconds
    FROM page_views

    {% if is_incremental() %}

    AND uploaded_at > (SELECT max(page_view_end_at) FROM {{ this }})

    {% endif %}
)

, page_views_w_dim AS (

    SELECT
      -- surrogate_key
      {{ dbt_utils.surrogate_key(['event_id','event_name']) }}                      AS fct_website_page_sk,

      -- dimension_keys
      dim_website_page_sk,
      
      -- natural_keys
      namespace_nk,
      project_nk,
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- other attributes
      event_name,
      NULL                                                                          AS sf_formid,
      page_view_start_at,
      page_view_end_at,
      engaged_seconds,
      NULL                                                                          AS collector_tstamp
    FROM page_views_w_clean_url
    LEFT JOIN dim_website_page ON page_views_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND page_views_w_clean_url.page_url_host = dim_website_page.page_url_host

)

, unstruct_w_clean_url AS (
    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS namespace_nk,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS project_nk,
      session_id,
      event_id,
      user_snowplow_domain_id,
      event_name,
      sf_formid,
      NULL                                                                          AS page_view_start_at,
      NULL                                                                          AS page_view_end_at,
      NULL                                                                          AS engaged_seconds,
      collector_tstamp
    FROM unstruct_events

    {% if is_incremental() %}

    AND collector_tstamp > (SELECT max(collector_tstamp) FROM {{ this }})

    {% endif %}
)

, unstruct_w_dim AS (
    SELECT
      -- surrogate_key
      {{ dbt_utils.surrogate_key(['event_id','event_name']) }}                      AS fct_website_page_sk,

      -- dimension_keys
      dim_website_page_sk,

      -- natural_keys
      namespace_nk,
      project_nk,
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- other attributes
      event_name,
      sf_formid,
      page_view_start_at,
      page_view_end_at,
      engaged_seconds,
      collector_tstamp
    FROM unstruct_w_clean_url
    LEFT JOIN dim_website_page ON unstruct_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND unstruct_w_clean_url.page_url_host = dim_website_page.page_url_host
)

SELECT *
FROM page_views_w_dim

UNION ALL

SELECT  *
FROM unstruct_w_dim
