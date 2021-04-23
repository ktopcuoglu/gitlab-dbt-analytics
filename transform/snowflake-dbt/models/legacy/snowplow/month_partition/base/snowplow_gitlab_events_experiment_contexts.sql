{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

WITH base as (

    SELECT DISTINCT 
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

), events_with_context_flattened AS (


    SELECT 
      base.*,
      f.value['schema']::VARCHAR         AS context_data_schema,
      TRY_PARSE_JSON(f.value['data'])    AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

), experiment_contexts AS (

    SELECT
      event_id,
      context_data:experiment::VARCHAR AS experiment_name,
      context_data:key::VARCHAR        AS context_key,
      context_data:variant::VARCHAR    AS experiment_variant
    FROM events_with_context_flattened
    WHERE context_data_schema ILIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%'
  
)

SELECT *
FROM experiment_contexts
