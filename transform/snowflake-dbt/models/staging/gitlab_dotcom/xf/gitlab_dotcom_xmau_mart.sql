{%- set event_ctes = [
  {
    "event_name": "action_monthly_active_users_project_repo",
    "events_to_include": "action_monthly_active_users_project_repo",
    "stage_name": "create",
    "smau": "True",
    "group": "gitaly",
    "gmau": "True"
  },
  {
    "event_name": "issue_interaction",
    "source_cte_name": "issue_interaction",
    "events_to_include": ["epics", "epic_notes"],
    "stage_name": "create",
    "smau": "False",
    "group": "source_code",
    "gmau": "True"
  }
]
-%}
WITH epic_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }}
    WHERE event_name IN ('epics', 'epic_notes')

)

, issue_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }}
    WHERE event_name IN ('issues', 'issue_notes')

)

, merge_request_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }}
    WHERE event_name IN ('merge_requests', 'merge_request_notes')
    
)

, unioned AS (

    SELECT *
    FROM epic_interaction

    UNION 

    SELECT *
    FROM issue_interaction

    UNION

    SELECT *
    FROM merge_request_interaction

)

, skeleton AS (

    SELECT DISTINCT first_day_of_month, last_day_of_month
    FROM {{ ref('date_details') }}
    WHERE date_day = last_day_of_month
        AND last_day_of_month < CURRENT_DATE()

)

, joined AS (

    SELECT 
      first_day_of_month,
      event_name,
      COUNT(DISTINCT user_id)
    FROM date_details
    LEFT JOIN unioned
        ON event_date BETWEEN DATEADD('days', -28, last_day_of_month) AND last_day_of_month

)

SELECT *
FROM joined
