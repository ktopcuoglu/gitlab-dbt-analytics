{{ config(
        materialized = "incremental",
        unique_key = "website_page_view_pk"
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
      app_id,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS dim_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS dim_project_id,
      session_id,
      user_snowplow_domain_id,
      page_view_id                                                                  AS event_id,
      'page_view'                                                                   AS event_name,
      min_tstamp                                                                    AS page_view_start_at,
      max_tstamp                                                                    AS page_view_end_at,
      time_engaged_in_s                                                             AS engaged_seconds
    FROM page_views

    {% if is_incremental() %}

    WHERE max_tstamp > (SELECT max(page_view_end_at) FROM {{ this }})

    {% endif %}

), page_views_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','page_view_end_at']) }}                AS website_page_view_pk,

      -- Foreign Keys
      dim_website_page_sk,
      dim_namespace_id,
      dim_project_id,

      --Time Attributes
      page_view_start_at,
      page_view_end_at,
      NULL                                                                          AS collector_tstamp,

      -- Natural Keys
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- Attributes
      event_name,
      NULL                                                                          AS sf_formid,
      engaged_seconds
    FROM page_views_w_clean_url
    LEFT JOIN dim_website_page ON page_views_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND page_views_w_clean_url.page_url_host = dim_website_page.page_url_host
    AND page_views_w_clean_url.app_id = dim_website_page.app_id

)

, unstruct_w_clean_url AS (

    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      app_id,
      page_url_host,
      REGEXP_SUBSTR(page_url_path, 'namespace(\\d+)', 1, 1, 'e', 1)                 AS dim_namespace_id,
      REGEXP_SUBSTR(page_url_path, 'project(\\d+)', 1, 1, 'e', 1)                   AS dim_project_id,
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

    WHERE collector_tstamp > (SELECT max(collector_tstamp) FROM {{ this }})

    {% endif %}

)

, unstruct_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.surrogate_key(['event_id','collector_tstamp']) }}                AS website_page_view_pk,

      -- Foreign Keys
      dim_website_page_sk,
      dim_namespace_id,
      dim_project_id,

      --Time Attributes
      page_view_start_at,
      page_view_end_at,
      collector_tstamp,

      -- Natural Keys
      session_id,
      event_id,
      user_snowplow_domain_id,

      -- Attributes
      event_name,
      sf_formid,
      engaged_seconds
    FROM unstruct_w_clean_url
    LEFT JOIN dim_website_page ON unstruct_w_clean_url.clean_url_path = dim_website_page.clean_url_path
    AND unstruct_w_clean_url.page_url_host = dim_website_page.page_url_host
    AND unstruct_w_clean_url.app_id = dim_website_page.app_id

)

, unioned AS (
    SELECT *
    FROM page_views_w_dim

    UNION ALL

    SELECT  *
    FROM unstruct_w_dim
)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-07-22",
    updated_date="2022-08-05"
) }}
