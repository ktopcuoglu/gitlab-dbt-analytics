{%- set event_ctes = [
  {
    "event_name": "events",
    "source_cte_name": "dim_event",
    "user_column_name": "dim_user_id",
    "project_column_name": "dim_project_id",
    "namespace_column_name": "dim_namespace_id",
    "primary_key": "dim_event_id",
    "lowest_grain_available": "project"
  }
]

-%}

{{ simple_cte([
    ('dim_event', 'dim_event'),
    ('dim_project', 'dim_project')
]) }}

, data AS (

{% for event_cte in event_ctes %}

    SELECT
      MD5({{ event_cte.source_cte_name}}.{{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}')   AS event_primary_key,
      '{{ event_cte.event_name }}'                                                                        AS event_name,
      {{ event_cte.source_cte_name}}.dim_project_id,
      {{ event_cte.source_cte_name}}.ultimate_parent_namespace_id,
      {{ event_cte.source_cte_name}}.dim_plan_id,
      {{ event_cte.source_cte_name}}.created_at,
      {{ event_cte.source_cte_name}}.created_date_id,
      {{ event_cte.source_cte_name}}.{{ event_cte.user_column_name }} AS dim_user_id,
      dim_project.is_imported AS project_is_imported
    FROM {{ event_cte.source_cte_name }}
    {% if event_cte.key_to_parent_project != 'NULL' %}
    LEFT JOIN dim_project ON {{event_cte.source_cte_name}}.{{event_cte.project_column_name}} = dim_project.dim_project_id
    {% endif %}
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor -%}

)

SELECT *
FROM data
