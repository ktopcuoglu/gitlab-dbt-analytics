{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"event_id"
  })
}}

WITH filtered_source as (

    SELECT
        event_id,
        contexts
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
    we need to extract the GitLab standard context fields from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The GitLab standard context which we are looking for is defined by schema at:
    https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/gitlab_standard/jsonschema/1-0-5

    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data
    */
    SELECT 
      base.*,
      f.value['schema']::VARCHAR  AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the gitlab standard context, which has this context schema: iglu:com.gitlab/gitlab_standard/jsonschema/1-0-5
Then we extract the id from the context_data column
*/
SELECT
    events_with_context_flattened.event_id::VARCHAR        AS event_id,
    context_data_schema,
    context_data['environment']::VARCHAR                   AS environment,
    TRY_PARSE_JSON(context_data['extra'])::VARIANT         AS extra,
    context_data['namespace_id']::NUMBER                   AS namespace_id,
    context_data['plan']::VARCHAR                          AS plan,
    context_data['google_analytics_id']::VARCHAR           AS google_analytics_id,
    IFF(google_analytics_id = '', NULL,
        SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
        SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR  AS google_analytics_client_id,
    context_data['project_id']::NUMBER                     AS project_id,
    context_data['user_id']::VARCHAR                       AS pseudonymized_user_id,
    context_data['source']::VARCHAR                        AS source
FROM events_with_context_flattened
WHERE context_data_schema like 'iglu:com.gitlab/gitlab_standard/jsonschema/%'
