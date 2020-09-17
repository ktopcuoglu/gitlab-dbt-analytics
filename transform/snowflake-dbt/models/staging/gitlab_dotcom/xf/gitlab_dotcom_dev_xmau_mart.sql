{%- set event_ctes = [
  {
    "event_name": "action_monthly_active_users_project_repo",
    "events_to_include": ["action_monthly_active_users_project_repo"],
    "stage_name": "create",
    "smau": "True",
    "group_name": "gitaly",
    "gmau": "True"
  },
  {
    "event_name": "epic_interaction",
    "events_to_include": ["epics", "epic_notes"],
    "stage_name": "plan",
    "smau": "False",
    "group_name": "portfolio_management",
    "gmau": "True"
  },
  {
    "event_name": "issue_interaction",
    "events_to_include": ["issues", "issue_notes"],
    "stage_name": "plan",
    "smau": "True",
    "group_name": "project_management",
    "gmau": "True"
  },
  {
    "event_name": "merge_request_interaction",
    "events_to_include": ["merge_requests", "merge_request_notes"],
    "stage_name": "create",
    "smau": "False",
    "group_name": "source_code",
    "gmau": "True"
  },
  {
    "event_name": "requirements",
    "events_to_include": ["requirements"],
    "stage_name": "plan",
    "smau": "False",
    "group_name": "certify",
    "gmau": "True"
  },
  {
    "event_name": "snippets",
    "events_to_include": ["snippets"],
    "stage_name": "create",
    "smau": "False",
    "group_name": "editor",
    "gmau": "True"
  }
]
-%}

WITH skeleton AS (

    SELECT DISTINCT first_day_of_month, last_day_of_month
    FROM {{ ref('date_details') }}
    WHERE date_day = last_day_of_month
        AND last_day_of_month < CURRENT_DATE()

)

{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (

    SELECT 
      user_id,
      namespace_id,
      event_date,
      plan_name_at_event_date,
      plan_id_at_event_date,
      namespace_is_internal,
      '{{ event_cte.event_name }}'       AS event_name,
      '{{ event_cte.stage_name }}'       AS stage_name,
      {{ event_cte.smau }}::BOOLEAN      AS is_smau,
      '{{ event_cte.group_name }}'       AS group_name,
      {{ event_cte.gmau }}::
      BOOLEAN      AS is_gmau
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }}
    WHERE event_name IN (
      {% for event_to_include in event_cte.events_to_include %}
        '{{ event_to_include }}'
        {% if not loop.last %},{% endif %}
      {% endfor %}
    )
    )
{% endfor -%}

, unioned AS (
    {% for event_cte in event_ctes %}
    SELECT *
    FROM {{ event_cte.event_name }}
    {% if not loop.last %}UNION{% endif %}
    {% endfor %}

)

, joined AS (

    SELECT 
      first_day_of_month,
      event_name,
      stage_name,
      is_smau,
      group_name,
      is_gmau,
      COUNT(DISTINCT user_id)                                                                           AS total_user_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date='free',user_id, NULL))                                 AS free_user_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date IN ('bronze', 'silver', 'gold'), user_id, NULL))       AS paid_user_count,
      COUNT(DISTINCT namespace_id)                                                                      AS total_namespace_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date='free',namespace_id, NULL))                            AS free_namespace_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date IN ('bronze', 'silver', 'gold'), namespace_id, NULL))  AS paid_namespace_count
    FROM skeleton
    LEFT JOIN unioned
        ON event_date BETWEEN DATEADD('days', -28, last_day_of_month) AND last_day_of_month
    {{ dbt_utils.group_by(n=6)}}

)

SELECT *
FROM joined
