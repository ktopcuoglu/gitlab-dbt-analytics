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
            AND app_id = 'gitlab-staging'
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

)

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the web_page context, which has this context schema: iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0
Then we extract the id from the context_data column
*/
SELECT 
    events_with_context_flattened.event_id,
    context_data['id']::TEXT AS web_page_id
FROM events_with_context_flattened
WHERE context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
