{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

WITH base AS (

  SELECT DISTINCT
    event_id,
    contexts
  {% if target.name not in ("prod") -%}

  FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

  {%- else %}

  FROM {{ ref('snowplow_gitlab_good_events_source') }}

  {%- endif %}

  WHERE app_id IS NOT NULL
    AND DATE_PART(MONTH, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ month_value }}'
    AND DATE_PART(YEAR, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ year_value }}'
    AND (
      (v_tracker LIKE 'js%' ) -- js frontend tracker
      OR (v_tracker LIKE 'rb%') -- ruby backend tracker
    )
    AND TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL

),

events_with_context_flattened AS (

  SELECT
    base.*,
    flat_contexts.value['schema']::VARCHAR AS context_data_schema,
    TRY_PARSE_JSON(flat_contexts.value['data']) AS context_data
  FROM base
  INNER JOIN LATERAL FLATTEN(input => TRY_PARSE_JSON(contexts), path => 'data') AS flat_contexts

),

experiment_contexts AS (

  SELECT DISTINCT -- Some event_id are not unique dispite haveing the same experiment context as discussed in MR 6288
    event_id,
    context_data['experiment']::VARCHAR AS experiment_name,
    context_data['key']::VARCHAR AS context_key,
    context_data['variant']::VARCHAR AS experiment_variant,
    ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ') AS experiment_migration_keys
  FROM events_with_context_flattened
  WHERE LOWER(context_data_schema) LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%'

)

SELECT *
FROM experiment_contexts
